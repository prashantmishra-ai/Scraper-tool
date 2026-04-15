from pymongo import MongoClient
import os

# Using the provided connection string via environment variable or default fallback
# The directConnection=true flag helps connect straight to the primary.
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://root:QWZzvaRfhaQgAvFYosm08bKrusELUBeN67zlQ3XR43R27aLbhqYBLZPAa2eIF5PN@ncrjyq53geq1emx68n14cuyd:27017/?directConnection=true"
)

client = MongoClient(MONGO_URI)
db = client['scraper_db']

isbn_collection = db['isbn_data']
generic_collection = db['generic_data']
