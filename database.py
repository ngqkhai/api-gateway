from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
import logging
from typing import List, Dict, Any
from bson import ObjectId
from config import MONGO_URI, MONGO_DB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB configuration
MONGO_COLLECTIONS = {
    "styles": "configurations_styles",
    "languages": "configurations_languages",
    "voices": "configurations_voices",
    "visual_styles": "configurations_visual_styles",
    "target_audience": "configurations_target_audience",
    "durations": "configurations_durations"
}

# Initialize MongoDB client
client = None
db = None

async def connect_to_mongodb():
    """Connect to MongoDB and initialize the database"""
    global client, db
    try:
        client = AsyncIOMotorClient(MONGO_URI)
        db = client[MONGO_DB]
        logger.info("Connected to MongoDB")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

async def close_mongodb_connection():
    """Close the MongoDB connection"""
    global client
    if client:
        logger.info("Closing MongoDB connection")
        client.close()

async def get_configurations(config_type: str) -> List[Dict[str, Any]]:
    """Get configuration items from MongoDB
    
    Args:
        config_type: Type of configuration to retrieve (styles, languages, etc.)
        
    Returns:
        List of configuration items with proper ID field
    """
    global db
    if db is None:
        raise RuntimeError("Database connection not established")
    
    try:
        # Get the appropriate collection name
        collection_name = MONGO_COLLECTIONS.get(config_type)
        if not collection_name:
            logger.warning(f"No collection mapping found for config_type: {config_type}")
            return []
            
        collection = db[collection_name]
        cursor = collection.find({})
        items = await cursor.to_list(length=100)
        
        if not items:
            logger.warning(f"No configurations found in collection: {collection_name}")
            return []
        
        # Transform items to ensure they have an id field
        result = []
        for item in items:
            # If the item has _id, use that as id
            if "_id" in item:
                item["id"] = str(item["_id"])
                del item["_id"]
            # If no id exists, create one based on name
            elif "id" not in item and "name" in item:
                item["id"] = item["name"].lower().replace(" ", "_")
            # Ensure there's some id
            elif "id" not in item:
                item["id"] = str(ObjectId())
                
            result.append(item)
            
        return result
    except Exception as e:
        logger.error(f"Error fetching {config_type} configurations: {str(e)}")
        raise

async def add_configuration(config_type: str, configuration: dict):
    """Add a new configuration"""
    if config_type not in MONGO_COLLECTIONS:
        raise ValueError(f"Invalid configuration type: {config_type}")
    
    collection = db[MONGO_COLLECTIONS[config_type]]
    result = await collection.insert_one(configuration)
    return result.inserted_id

async def update_configuration(config_type: str, config_id: str, configuration: dict):
    """Update an existing configuration"""
    if config_type not in MONGO_COLLECTIONS:
        raise ValueError(f"Invalid configuration type: {config_type}")
    
    collection = db[MONGO_COLLECTIONS[config_type]]
    result = await collection.update_one(
        {"id": config_id},
        {"$set": configuration}
    )
    return result.modified_count > 0

async def delete_configuration(config_type: str, config_id: str):
    """Delete a configuration"""
    if config_type not in MONGO_COLLECTIONS:
        raise ValueError(f"Invalid configuration type: {config_type}")
    
    collection = db[MONGO_COLLECTIONS[config_type]]
    result = await collection.delete_one({"id": config_id})
    return result.deleted_count > 0 