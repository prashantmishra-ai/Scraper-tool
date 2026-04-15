#!/usr/bin/env python3
"""
Quick test script to verify MongoDB connection
Run this to diagnose database connectivity issues
"""

import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

def test_connection(uri, name):
    """Test a MongoDB connection"""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"{'='*60}")
    
    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000
        )
        
        # Try to ping
        client.admin.command('ping')
        print(f"✓ Connection successful!")
        
        # Try to list databases
        dbs = client.list_database_names()
        print(f"✓ Available databases: {', '.join(dbs)}")
        
        # Try to access scraper_db
        db = client['scraper_db']
        collections = db.list_collection_names()
        print(f"✓ Collections in scraper_db: {', '.join(collections) if collections else 'none yet'}")
        
        return True
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"✗ Connection failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    print("MongoDB Connection Test")
    print("="*60)
    
    # Test 1: Environment variable
    env_uri = os.environ.get("MONGO_URI")
    if env_uri:
        test_connection(env_uri, "Environment Variable (MONGO_URI)")
    else:
        print("\n⚠ MONGO_URI environment variable not set")
    
    # Test 2: Default remote URI
    default_uri = "mongodb://root:QWZzvaRfhaQgAvFYosm08bKrusELUBeN67zlQ3XR43R27aLbhqYBLZPAa2eIF5PN@ncrjyq53geq1emx68n14cuyd:27017/?directConnection=true"
    test_connection(default_uri, "Default Remote MongoDB")
    
    # Test 3: Localhost
    test_connection("mongodb://localhost:27017/", "Localhost MongoDB")
    
    print("\n" + "="*60)
    print("Test complete!")
    print("="*60)
