# main.py
import asyncio
import os
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import logging

import motor.motor_asyncio # Asynchronous MongoDB driver
import socketio # Asynchronous Socket.IO server
from fastapi import FastAPI, Response, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# --- Configuration ---
# Use environment variable for MongoDB URI, default to local for development
# For production, set MONGO_URI in your environment (e.g., "mongodb://user:pass@host:port/db_name")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "sensors_db"
COLLECTION_NAME = "readings"

# Thresholds (matching frontend)
TEMP_LCL = 22
TEMP_UCL = 32
HUM_LCL = 50
HUM_UCL = 75

# Sensor and Zone mapping (matching frontend's conceptual structure)
# This defines which sensors belong to which logical zone.
ZONES_CONFIG = {
    'MI': ['S1', 'S2'],
    'FATP': ['S3'],
    'TRC': ['S4'],
    'Aging 1': ['S5'],
    'Aging 2': ['S6']
}

# Ordered list of zone names to match frontend's dataStore indexing
FRONTEND_ZONE_ORDER = ['MI', 'FATP', 'TRC', 'Aging 1', 'Aging 2']

# Reverse mapping for quick lookup: sensor_id -> zone_name
SENSOR_TO_ZONE = {sensor_id: zone_name for zone_name, sensors in ZONES_CONFIG.items() for sensor_id in sensors}

# Setup logging for better observability in production
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- In-memory fallback collection ---
class InMemoryCollection:
    """A tiny async-friendly in-memory collection used when MongoDB is unavailable.
    Implements the minimal interface used by this app: create_index, insert_one,
    find, aggregate and a simple to_list on cursors.
    """
    def __init__(self):
        self._docs = []

    async def create_index(self, *args, **kwargs):
        return None

    async def insert_one(self, doc):
        # ensure timestamp is a datetime for downstream code
        self._docs.append(doc)
        class _Res: pass
        return _Res()

    def find(self, *args, **kwargs):
        # Return a simple cursor-like object supporting sort().limit().to_list()
        docs = list(self._docs)

        class Cursor:
            def __init__(self, docs):
                self._docs = docs
                self._limit = None

            def sort(self, *a, **k):
                # naive sort support: if sorting by timestamp descending
                try:
                    if a and isinstance(a[0], tuple) and a[0][0] == 'timestamp':
                        direction = a[0][1]
                        self._docs.sort(key=lambda d: d.get('timestamp', None), reverse=(direction == -1))
                except Exception:
                    pass
                return self

            def limit(self, n):
                self._limit = n
                return self

            async def to_list(self, length=None):
                if self._limit is not None:
                    return self._docs[:self._limit]
                return list(self._docs)

        return Cursor(docs)

    def aggregate(self, pipeline):
        # Provide a simple aggregation compatible with get_history pipeline used
        # Group by second and zone, pick first reading for that second/zone
        # Build mapping
        from collections import OrderedDict, defaultdict
        grouped = {}
        for d in sorted(self._docs, key=lambda x: x.get('timestamp', None)):
            ts = d.get('timestamp')
            if ts is None:
                continue
            sec = ts.strftime('%Y-%m-%d %H:%M:%S')
            zone = d.get('zone')
            key = (sec, zone)
            if key not in grouped:
                grouped[key] = {
                    '_id': {'zone': zone, 'second': sec},
                    'temperature': d.get('temperature'),
                    'humidity': d.get('humidity'),
                    'timestamp': d.get('timestamp')
                }

        class AggCursor:
            def __init__(self, items):
                self._items = list(items)

            async def to_list(self, length=None):
                return self._items

        # Return results sorted by timestamp
        items = sorted(grouped.values(), key=lambda x: x['timestamp'])
        return AggCursor(items)


# --- FastAPI App and Socket.IO Setup ---
app = FastAPI(
    title="Dixon Temperature and Humidity Live Monitoring System",
    description="Real-time temperature and humidity monitoring system for Dixon.",
    version="1.0.0",
)

# Socket.IO server instance
sio = socketio.AsyncServer(cors_allowed_origins="*", async_mode="asgi") # Allow all origins for development
# Wrap FastAPI app with Socket.IO ASGI middleware
sio_app = socketio.ASGIApp(sio, app)

# MongoDB client, database, and collection instances
mongo_client: motor.motor_asyncio.AsyncIOMotorClient = None
db: motor.motor_asyncio.AsyncIOMotorDatabase = None
readings_collection: motor.motor_asyncio.AsyncIOMotorCollection = None

# --- Pydantic Models for Data Validation ---
class SensorReading(BaseModel):
    """Model for a single sensor reading stored in MongoDB."""
    sensor_id: str
    zone: str
    temperature: float
    humidity: float
    timestamp: datetime = Field(default_factory=datetime.utcnow) # Use UTC for consistency

class SummaryEvent(BaseModel):
    """Model for a summary event sent to the frontend."""
    ts: float # Unix timestamp in milliseconds
    sensor: str
    zone: str
    temp: float
    hum: float
    status: str

class HistoryResponse(BaseModel):
    """Model for the historical chart data response."""
    temp_data: list[list[float | None]] # Allow None for missing data points
    hum_data: list[list[float | None]] # Allow None for missing data points

# --- Database Connection Lifecycle ---
@app.on_event("startup")
async def startup_db_client():
    """Connect to MongoDB and create necessary indexes on application startup."""
    global mongo_client, db, readings_collection
    try:
        mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        # quick ping to ensure connection
        await mongo_client.server_info()
        db = mongo_client[DB_NAME]
        readings_collection = db[COLLECTION_NAME]
        # Ensure indexes for faster queries
        await readings_collection.create_index([("timestamp", -1)])
        await readings_collection.create_index([("zone", 1), ("timestamp", -1)])
        logger.info(f"Connected to MongoDB at {MONGO_URI}, database {DB_NAME}")
    except Exception as e:
        # If MongoDB is not available, fallback to an in-memory collection for demo/testing.
        logger.warning(f"Could not connect to MongoDB ({e}). Using in-memory fallback.")
        readings_collection = InMemoryCollection()

@app.on_event("shutdown")
async def shutdown_db_client():
    """Close MongoDB connection on application shutdown."""
    if mongo_client:
        mongo_client.close()
        logger.info("Disconnected from MongoDB.")
    else:
        logger.info("Shutdown: no MongoDB client to close (using in-memory fallback or never connected).")

# --- Utility Functions (matching frontend logic) ---
def rand_temp():
    """Generates a random temperature reading within a reasonable range."""
    return round(random.uniform(TEMP_LCL - 2, TEMP_UCL + 2), 1)

def rand_hum():
    """Generates a random humidity reading within a reasonable range."""
    return round(random.uniform(HUM_LCL - 10, HUM_UCL + 10), 1)

def status_for_metric(value: float, lcl: float, ucl: float) -> str:
    """Determines status (OK, WARN, CRITICAL) for a single metric."""
    if value < lcl - 3 or value > ucl + 3: return 'CRITICAL'
    if value < lcl or value > ucl: return 'WARN'
    return 'OK'

def combine_status(temp: float, hum: float) -> str:
    """Combines temperature and humidity statuses, returning the highest severity."""
    t_severity = 0
    if temp < TEMP_LCL - 3 or temp > TEMP_UCL + 3: t_severity = 3
    elif temp < TEMP_LCL or temp > TEMP_UCL: t_severity = 2
    else: t_severity = 1

    h_severity = 0
    if hum < HUM_LCL - 10 or hum > HUM_UCL + 10: h_severity = 3
    elif hum < HUM_LCL or hum > HUM_UCL: h_severity = 2
    else: h_severity = 1

    if t_severity >= h_severity:
        return status_for_metric(temp, TEMP_LCL, TEMP_UCL)
    else:
        return status_for_metric(hum, HUM_LCL, HUM_UCL)

# --- Background Sensor Data Simulator ---
async def background_sensor_simulator():
    """
    Asynchronously simulates real-time sensor data, inserts into MongoDB,
    and emits updates via Socket.IO to connected clients.
    """
    logger.info("Starting background sensor simulator...")
    while True:
        try:
            # Generate one temp/hum reading per logical zone
            zone_current_readings = {}
            for zone_name in FRONTEND_ZONE_ORDER:
                zone_current_readings[zone_name] = {
                    'temp': rand_temp(),
                    'hum': rand_hum()
                }

            # For each sensor, use the generated reading for its assigned zone
            for zone_name, sensors_in_zone in ZONES_CONFIG.items():
                current_zone_temp = zone_current_readings[zone_name]['temp']
                current_zone_hum = zone_current_readings[zone_name]['hum']
                current_timestamp = datetime.utcnow()

                for sensor_id in sensors_in_zone:
                    reading = SensorReading(
                        sensor_id=sensor_id,
                        zone=zone_name,
                        temperature=current_zone_temp,
                        humidity=current_zone_hum,
                        timestamp=current_timestamp
                    )
                    await readings_collection.insert_one(reading.dict())

                    status = combine_status(current_zone_temp, current_zone_hum)

                    # Emit real-time data to connected clients
                    await sio.emit('new_reading', {
                        'sensor_id': sensor_id,
                        'zone': zone_name,
                        'temperature': current_zone_temp,
                        'humidity': current_zone_hum,
                        'status': status,
                        'timestamp': reading.timestamp.isoformat() # ISO format for JS Date parsing
                    })
            await asyncio.sleep(1) # Simulate the frontend's TICK_MS (1 second)
        except Exception as e:
            logger.exception(f"Error in background sensor simulator: {e}")
            await asyncio.sleep(5) # Wait before retrying to prevent rapid error loops

# Start the background task on app startup
@app.on_event("startup")
async def start_background_tasks():
    """Schedules the sensor simulator as a background task."""
    asyncio.create_task(background_sensor_simulator())
    logger.info("Background sensor simulator task scheduled.")

# --- API Endpoints ---
@app.get("/api/history", response_model=HistoryResponse, summary="Get historical data for charts")
async def get_history():
    """
    Fetches historical sensor data for the last 10 minutes, aggregated by zone and second,
    and formats it for the frontend's chart display.
    """
    try:
        time_ago = datetime.utcnow() - timedelta(minutes=10) # Fetch last 10 minutes of data
        
        # MongoDB aggregation pipeline to group by second and zone, taking the first reading
        # This ensures one data point per zone per second for charting.
        pipeline = [
            {"$match": {"timestamp": {"$gte": time_ago}}},
            {"$sort": {"timestamp": 1}}, # Sort by timestamp ascending
            {"$group": {
                "_id": {
                    "zone": "$zone",
                    "second": {"$dateToString": {"format": "%Y-%m-%d %H:%M:%S", "date": "$timestamp"}}
                },
                "temperature": {"$first": "$temperature"}, # Take first reading for that second/zone
                "humidity": {"$first": "$humidity"},
                "timestamp": {"$first": "$timestamp"} # Keep the actual timestamp for sorting
            }},
            {"$sort": {"timestamp": 1}} # Sort by actual timestamp for chronological order
        ]
        
        readings_cursor = readings_collection.aggregate(pipeline)
        readings = await readings_cursor.to_list(length=None) # Fetch all results

        # Initialize lists for chart data, matching frontend's dataStore structure
        zone_data_temp = [[] for _ in FRONTEND_ZONE_ORDER]
        zone_data_hum = [[] for _ in FRONTEND_ZONE_ORDER]

        # Collect all unique timestamps to ensure consistent labels across charts
        all_timestamps = sorted(list(set(r['_id']['second'] for r in readings)))
        
        # Create a map for quick lookup: { "YYYY-MM-DD HH:MM:SS": { "ZoneName": { "temp": val, "hum": val } } }
        data_by_timestamp_and_zone = {}
        for r in readings:
            ts_str = r['_id']['second']
            zone = r['_id']['zone']
            if ts_str not in data_by_timestamp_and_zone:
                data_by_timestamp_and_zone[ts_str] = {}
            data_by_timestamp_and_zone[ts_str][zone] = {
                'temp': r['temperature'],
                'hum': r['humidity']
            }

        # Populate the ordered lists for frontend charts
        for ts_str in all_timestamps:
            for idx, zone_name in enumerate(FRONTEND_ZONE_ORDER):
                if zone_name in data_by_timestamp_and_zone.get(ts_str, {}):
                    zone_data_temp[idx].append(data_by_timestamp_and_zone[ts_str][zone_name]['temp'])
                    # Aging zones (idx 3, 4) only have temp charts, so their hum data is not used.
                    # We send None to maintain array length consistency if the frontend expects it.
                    if zone_name not in ['Aging 1', 'Aging 2']:
                        zone_data_hum[idx].append(data_by_timestamp_and_zone[ts_str][zone_name]['hum'])
                    else:
                        zone_data_hum[idx].append(None) # Explicitly send None for humidity for these zones
                else:
                    # If a zone has no data for a specific timestamp, append None.
                    # Frontend's Chart.js can handle null or undefined for gaps.
                    zone_data_temp[idx].append(None)
                    zone_data_hum[idx].append(None)

        return HistoryResponse(temp_data=zone_data_temp, hum_data=zone_data_hum)
    except Exception as e:
        logger.exception(f"Error fetching historical data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error fetching history")

@app.get("/api/summary_events", response_model=list[SummaryEvent], summary="Get recent summary events for table")
async def get_summary_events():
    """
    Returns the latest 200 summary events for the table, ordered by timestamp descending.
    """
    try:
        # Fetch the latest 200 readings, ordered by timestamp descending
        readings_cursor = readings_collection.find().sort("timestamp", -1).limit(200)
        readings = await readings_cursor.to_list(length=200)

        events = []
        for doc in readings:
            temp = doc['temperature']
            hum = doc['humidity']
            status = combine_status(temp, hum)
            events.append(SummaryEvent(
                ts=doc['timestamp'].timestamp() * 1000, # Convert to JS Unix timestamp (ms)
                sensor=doc['sensor_id'],
                zone=doc['zone'],
                temp=temp,
                hum=hum,
                status=status
            ))
        return events
    except Exception as e:
        logger.exception(f"Error fetching summary events: {e}")
        raise HTTPException(status_code=500, detail="Internal server error fetching summary events")

@app.get("/api/export_csv", summary="Export recent sensor data as CSV")
async def export_csv():
    """
    Exports recent sensor readings (up to 2000) as a CSV file.
    """
    try:
        readings_cursor = readings_collection.find().sort("timestamp", -1).limit(2000)
        readings = await readings_cursor.to_list(length=2000)

        csv_data = StringIO()
        csv_data.write("Time,Sensor,Zone,Temp (°C),Humidity (%),Status\n")
        for doc in readings:
            temp = doc['temperature']
            hum = doc['humidity']
            status = combine_status(temp, hum)
            formatted_time = doc['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            csv_data.write(f'"{formatted_time}",' \
                           f'"{doc["sensor_id"]}",' \
                           f'"{doc["zone"]}",' \
                           f'{temp},' \
                           f'{hum},' \
                           f'{status}\n')

        csv_data.seek(0)
        
        filename = f'sensor_readings_{int(time.time())}.csv'
        return StreamingResponse(
            csv_data,
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logger.exception(f"Error exporting CSV: {e}")
        raise HTTPException(status_code=500, detail="Internal server error exporting CSV")

# --- Serve Static Files (index.html) ---
# Mount the static files directory. Assuming index.html is in the same directory as main.py
app.mount("/", StaticFiles(directory=".", html=True), name="static")

# --- Socket.IO Event Handlers (optional, for custom events) ---
@sio.event
async def connect(sid, environ):
    logger.info(f"Client connected: {sid}")

@sio.event
async def disconnect(sid):
    logger.info(f"Client disconnected: {sid}")

# --- Main entry point for Uvicorn ---
# To run this application:
# 1. Install dependencies: pip install fastapi uvicorn[standard] python-socketio motor pydantic
# 2. Start MongoDB (e.g., mongod command if installed locally)
# 3. Run the server: uvicorn main:sio_app --reload --port 8000
#    The sio_app is the ASGI application that wraps both FastAPI and Socket.IO.