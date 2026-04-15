from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import os
import sys

# Using the provided connection string via environment variable or default fallback
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://root:QWZzvaRfhaQgAvFYosm08bKrusELUBeN67zlQ3XR43R27aLbhqYBLZPAa2eIF5PN@ncrjyq53geq1emx68n14cuyd:27017/?directConnection=true"
)

client = None
connection_status = "unknown"

# Try primary connection
try:
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=10000,
        retryWrites=True,
        retryReads=True,
        maxPoolSize=10
    )
    client.admin.command('ping')
    connection_status = "connected_remote"
    print("✓ MongoDB connected (remote)")
    
except (ConnectionFailure, ServerSelectionTimeoutError) as e:
    print(f"⚠ Remote MongoDB connection failed: {e}")
    print("⚠ Attempting localhost fallback...")
    
    # Try localhost fallback
    try:
        client = MongoClient(
            "mongodb://localhost:27017/",
            serverSelectionTimeoutMS=3000,
            connectTimeoutMS=5000
        )
        client.admin.command('ping')
        connection_status = "connected_localhost"
        print("✓ MongoDB connected (localhost)")
    except Exception as local_err:
        print(f"✗ Localhost MongoDB also failed: {local_err}")
        print("✗ Running without database - data will NOT be saved!")
        connection_status = "disconnected"
        # Create a dummy client that won't crash the app
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1000)

except Exception as e:
    print(f"⚠ Unexpected MongoDB error: {e}")
    connection_status = "error"
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1000)

db = client['scraper_db']
isbn_collection = db['isbn_data']
generic_collection = db['generic_data']

def is_db_connected():
    """Check if database is actually connected"""
    return connection_status in ["connected_remote", "connected_localhost"]
