from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
import logging
from typing import List, Dict, Any, Optional
from bson import ObjectId
from config import MONGO_URI, MONGO_DB
from datetime import datetime
import uuid

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
    "durations": "configurations_durations",
    "jobs": "jobs"  # Add jobs collection
}

# Job status constants
class JobStatus:
    PENDING = "PENDING"
    SCRIPT_GENERATED = "SCRIPT_GENERATED"
    VOICE_GENERATED = "VOICE_GENERATED"
    IMAGE_GENERATED = "IMAGE_GENERATED"
    READY = "READY"
    FAILED = "FAILED"

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

# Job-related database functions
async def create_job(job_data: Dict[str, Any]) -> str:
    """Create a new job in the database
    
    Args:
        job_data: Job data including request parameters
        
    Returns:
        The job ID as a string
    """
    global db
    if db is None:
        raise RuntimeError("Database connection not established")
        
    jobs_collection = db[MONGO_COLLECTIONS["jobs"]]
    
    # Extract existing job_id if it exists
    job_id = job_data.get("job_id") or job_data.get("script_id") or job_data.get("id")
    
    # If no job_id, generate a UUID
    if not job_id:
        job_id = str(uuid.uuid4())
    
    # Add timestamps and initial status
    job = {
        **job_data,
        "job_id": job_id,  # Store the string UUID as job_id
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        "status": JobStatus.PENDING
    }
    
    # Insert the job, which will generate an _id ObjectId
    result = await jobs_collection.insert_one(job)
    mongo_id = str(result.inserted_id)
    
    # Log both IDs for debugging
    logger.info(f"Created new job with MongoDB _id: {mongo_id}, job_id: {job_id}")
    
    # Return the UUID job_id (not the ObjectId) as the primary identifier
    return job_id

async def update_job_status(job_id: str, status: str, data: Optional[Dict[str, Any]] = None):
    """Update a job's status and optionally add data
    
    Args:
        job_id: The job ID (could be either job_id UUID or MongoDB _id)
        status: New status (use JobStatus constants)
        data: Optional data to update (script_text, audio_url, etc.)
    """
    global db
    if db is None:
        raise RuntimeError("Database connection not established")
        
    jobs_collection = db[MONGO_COLLECTIONS["jobs"]]
    
    # Prepare update document
    update_doc = {
        "status": status,
        "updated_at": datetime.utcnow()
    }
    
    # Add any additional data
    if data:
        update_doc.update(data)
    
    # Execute update - try with job_id field first
    try:
        # First try job_id field (preferred)
        result = await jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": update_doc}
        )
        
        # If not found by job_id, try _id as ObjectId
        if result.matched_count == 0:
            try:
                object_id = ObjectId(job_id)
                result = await jobs_collection.update_one(
                    {"_id": object_id},
                    {"$set": update_doc}
                )
            except Exception:
                # Not a valid ObjectId, try other fields
                pass
                
        # If still not found, try id field
        if result.matched_count == 0:
            result = await jobs_collection.update_one(
                {"id": job_id},
                {"$set": update_doc}
            )
        
        if result.matched_count == 0:
            logger.warning(f"Job not found with any ID format: {job_id}")
        else:
            logger.info(f"Updated job {job_id} status to {status}")
            
    except Exception as e:
        logger.error(f"Error updating job status: {str(e)}")
        raise

async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job by ID
    
    Args:
        job_id: The job ID (can be either ObjectId or job_id UUID string)
        
    Returns:
        The job document or None if not found
    """
    global db
    if db is None:
        raise RuntimeError("Database connection not established")
        
    jobs_collection = db[MONGO_COLLECTIONS["jobs"]]
    
    try:
        # First try using the job_id field
        job = await jobs_collection.find_one({"job_id": job_id})
        
        # If not found, try treating job_id as _id ObjectId
        if not job:
            try:
                job = await jobs_collection.find_one({"_id": ObjectId(job_id)})
            except Exception:
                # Not a valid ObjectId, try other fields
                pass
        
        # Try uuid field
        if not job:
            job = await jobs_collection.find_one({"uuid": job_id})
            
        # Try id field (some documents might store id instead of _id)
        if not job:
            job = await jobs_collection.find_one({"id": job_id})
            
        if job:
            # Convert _id to string
            if "_id" in job:
                job["id"] = str(job["_id"])
                del job["_id"]
            
            # Convert datetime objects to ISO format strings for JSON serialization
            for key in ["created_at", "updated_at"]:
                if key in job and isinstance(job[key], datetime):
                    job[key] = job[key].isoformat()
            
        return job
    except Exception as e:
        logger.error(f"Error fetching job: {str(e)}")
        return None

async def update_job_from_script_ready(job_id: str, data: Dict[str, Any]):
    """Update job with data from script.ready event
    
    Args:
        job_id: The job ID
        data: Data from script.ready event
    """
    global db
    if db is None:
        raise RuntimeError("Database connection not established")
        
    try:
        # Log initial data we received for debugging
        logger.info(f"Updating job {job_id} with data keys: {list(data.keys())}")
        
        # Prepare update data - focus only on structured data
        update_data = {
            "status": JobStatus.READY,
            "updated_at": datetime.utcnow(),
        }
        
        # Add script, voice_data, and image_data if present
        if "script" in data:
            update_data["script"] = data["script"]
            
        if "voice_data" in data:
            update_data["voice_data"] = data["voice_data"]
            
        if "image_data" in data:
            update_data["image_data"] = data["image_data"]
        
        # Log what we're updating
        logger.info(f"Update will include: script={bool('script' in update_data)}, " 
                    f"voice_data={bool('voice_data' in update_data)}, "
                    f"image_data={bool('image_data' in update_data)}")
        
        # Find and update the job - try different ID fields
        jobs_collection = db[MONGO_COLLECTIONS["jobs"]]
        
        # Log the job_id we're trying to update
        logger.info(f"Attempting to update job with ID: {job_id}")
        
        # Try matching using job_id field first
        result = await jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": update_data}
        )
        
        if result and result.matched_count > 0:
            logger.info(f"Updated job using job_id field: {job_id}")
            return
        
        # Try matching by _id as ObjectId
        try:
            object_id = ObjectId(job_id)
            result = await jobs_collection.update_one(
                {"_id": object_id},
                {"$set": update_data}
            )
            if result and result.matched_count > 0:
                logger.info(f"Updated job using _id as ObjectId: {job_id}")
                return
        except Exception as e:
            logger.warning(f"ID {job_id} is not a valid ObjectId: {str(e)}")
            
        # Try matching by UUID field
        result = await jobs_collection.update_one(
            {"uuid": job_id},
            {"$set": update_data}
        )
        
        if result and result.matched_count > 0:
            logger.info(f"Updated job using uuid field: {job_id}")
            return
            
        # Try matching by id field
        result = await jobs_collection.update_one(
            {"id": job_id},
            {"$set": update_data}
        )
        
        if result and result.matched_count > 0:
            logger.info(f"Updated job using id field: {job_id}")
            return
            
        # One last attempt - try collection_id
        collection_id = None
        if "script" in data and isinstance(data["script"], dict):
            collection_id = data["script"].get("collection_id")
        elif "collection_id" in data:
            collection_id = data["collection_id"]
                
        if collection_id:
            result = await jobs_collection.update_one(
                {"collection_id": collection_id},
                {"$set": update_data}
            )
            if result and result.matched_count > 0:
                logger.info(f"Updated job using collection_id: {collection_id}")
                return
                
        # No match found with any field
        logger.error(f"Job not found with any ID format. job_id: {job_id}, collection_id: {collection_id}")
        
    except Exception as e:
        logger.error(f"Error updating job with script.ready data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise 