from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form, BackgroundTasks, WebSocket, WebSocketDisconnect, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import logging
from typing import Dict, Any, List, Optional
import os
import json
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from database import (
    connect_to_mongodb, 
    close_mongodb_connection, 
    get_configurations, 
    create_job, 
    update_job_status, 
    get_job,
    update_job_from_script_ready,
    JobStatus,
    MONGO_DB,
    db
)
from message_broker import MessageBroker
from websocket import ConnectionManager
from config import (
    DATA_COLLECTOR_URL,
    SCRIPT_GENERATOR_URL,
    VOICE_SYNTHESIS_URL,
    VISUAL_GENERATION_URL,
    AGGREGATOR_URL,
    MONGO_URI,
    TIMEOUT,
    MAX_RETRIES,
    CORS_ORIGINS,
    LOG_LEVEL
)
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger(__name__)

# Validate required environment variables
required_env_vars = ["DATA_COLLECTOR_URL", "SCRIPT_GENERATOR_URL", "MONGO_URI"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# HTTP client configuration
TIMEOUT = 30.0  # seconds
MAX_RETRIES = 3

# Base Configuration Model
class Configuration(BaseModel):
    id: str
    name: str
    description: Optional[str] = None

# Style Configuration Model
class StyleConfiguration(BaseModel):
    id: str = Field(...)
    name: str
    description: Optional[str] = None

# Language Configuration Model
class LanguageConfiguration(BaseModel):
    id: str = Field(...)
    name: str
    description: Optional[str] = None
    encoded: Optional[str] = None

# Voice Configuration Model
class VoiceConfiguration(BaseModel):
    id: str = Field(...)
    name: str
    gender: Optional[str] = None
    description: Optional[str] = None
    cloudinary_url: Optional[str] = None
    sample_text: Optional[str] = None


# Visual Style Configuration Model
class VisualStyleConfiguration(BaseModel):
    id: str = Field(...)
    name: str
    description: Optional[str] = None

# Target Audience Configuration Model
class TargetAudienceConfiguration(BaseModel):
    id: str = Field(...)
    name: str
    description: Optional[str] = None

# Duration Configuration Model
class DurationConfiguration(BaseModel):
    id: str = Field(...)
    name: str
    description: Optional[str] = None
    
# Create WebSocket and message broker instances
ws_manager = ConnectionManager()
message_broker = MessageBroker()

# Script-ready event callback handler
async def handle_script_ready(data: Dict[str, Any]):
    """Handle a script.ready event from the RabbitMQ queue"""
    try:
        # Import necessary modules inside the function to avoid circular imports
        from database import db, MONGO_DB, get_job, create_job, JobStatus

        # If we received a string (possibly a Python repr with single quotes), try to parse it
        if isinstance(data, str):
            logger.info(f"Received string data, attempting to parse: {data[:100]}...")
            try:
                import ast
                data = ast.literal_eval(data)
            except (SyntaxError, ValueError) as e:
                logger.error(f"Failed to parse string data: {str(e)}")
                return
        
        # Log the complete data for debugging
        logger.info(f"Script ready event full data: {json.dumps(data, default=str)}")
        
        logger.info(f"Handling script.ready event data type: {type(data)}")
        
        # Extract script data
        script_data = data.get("script", {})
        logger.info(f"Extracted script_data: {json.dumps(script_data, default=str)}")
        
        # Try to extract various IDs
        job_id = None
        collection_id = None
        script_id = None
        
        # Check for _id in script
        if isinstance(script_data, dict):
            script_id = script_data.get("_id")
            collection_id = script_data.get("collection_id")
        
        # If job_id not found, check script_id in the data
        if isinstance(data, dict):
            if not script_id:
                script_id = data.get("script_id")
            
            # If collection_id not found, check in the data
            if not collection_id:
                collection_id = data.get("collection_id")
                
        # Convert IDs to proper format if needed
        if script_id:
            # If it's a UUID format, use it as-is for job_id
            if isinstance(script_id, str) and (len(script_id) == 36 or "-" in script_id):
                job_id = script_id
            else:
                # For ObjectId format, convert to string
                job_id = str(script_id)
        
        logger.info(f"Extracted IDs - job_id: {job_id}, collection_id: {collection_id}")
        
        if not job_id:
            logger.error("Missing job_id in script.ready event")
            if collection_id:
                # If we have a collection_id but no job_id, we can still update the collection
                logger.info(f"Sending collection update for collection_id: {collection_id}")
                await ws_manager.send_collection_update(collection_id, {
                    "type": "script_generated",
                    "collection_id": collection_id,
                    "status": "completed",
                    "progress": 100
                })
            return
            
        logger.info(f"Handling script.ready event for job_id: {job_id}")
        
        # Extract voice data
        voice_data = data.get("voice", {})
        logger.info(f"Voice data: {json.dumps(voice_data, default=str)}")
        if isinstance(voice_data, dict):
            audio_url = voice_data.get("audio_url")
            scene_voiceovers = voice_data.get("scene_voiceovers", [])
        else:
            audio_url = None
            scene_voiceovers = []
        
        # Extract image data
        image_data = data.get("image", {})
        logger.info(f"Image data: {json.dumps(image_data, default=str)}")
        if isinstance(image_data, dict):
            image_urls = []
            scene_images = image_data.get("scene_images", [])
            
            # Extract image URLs from scene_images
            for img in scene_images:
                if isinstance(img, dict) and "cloudinary_url" in img:
                    image_urls.append(img["cloudinary_url"])
                elif isinstance(img, dict) and "url" in img:
                    image_urls.append(img["url"])
        else:
            image_urls = []
            scene_images = []
        
        # Extract script text and scenes
        scenes = script_data.get("scenes", [])
        script_text = script_data.get("script_text", "")
        logger.info(f"Extracted script_text: {script_text[:100]}... (truncated)")
        logger.info(f"Extracted scenes count: {len(scenes)}")
        
        # If we don't have individual scenes but have script_text, create a single scene
        if not scenes and script_text:
            scenes = [{
                "scene_id": f"{job_id}_scene_1",
                "script": script_text,
                "visual": script_data.get("visual_description", "")
            }]
        
        # Look for content in different fields if the standard ones are empty
        if not script_text:
            # Try alternative field names
            script_text = script_data.get("content") or data.get("content") or data.get("text") or ""
            logger.info(f"Found alternative script_text: {script_text[:100]}... (truncated)")
            
        if not audio_url:
            # Try alternative field names for audio
            audio_url = voice_data.get("url") or data.get("audio_url") or ""
            logger.info(f"Found alternative audio_url: {audio_url}")
            
        if not image_urls and isinstance(image_data, dict):
            # Try alternative field names for images
            alt_images = image_data.get("images") or data.get("images") or []
            for img in alt_images:
                if isinstance(img, dict):
                    img_url = img.get("url") or img.get("cloudinary_url")
                    if img_url:
                        image_urls.append(img_url)
                elif isinstance(img, str):
                    image_urls.append(img)
            logger.info(f"Found alternative image_urls: {image_urls}")
            
        # Prepare update data with structured format
        update_data = {
            "status": JobStatus.READY,
            "updated_at": datetime.utcnow(),
            "script": {
                "_id": job_id,
                "collection_id": collection_id,
                "scenes": scenes,
                "metadata": script_data.get("metadata", {})
            },
            "voice_data": {
                "script_id": job_id,
                "collection_id": collection_id,
                "scene_voiceovers": scene_voiceovers
            },
            "image_data": {
                "script_id": job_id,
                "collection_id": collection_id,
                "scene_images": scene_images
            },
            "script_text": script_text,
            "audio_url": audio_url,
            "image_urls": image_urls
        }
        
        logger.info(f"Update data prepared with script_text: {'Present' if script_text else 'Missing'}")
        logger.info(f"Update data prepared with audio_url: {'Present' if audio_url else 'Missing'}")
        logger.info(f"Update data prepared with image_urls: {len(image_urls)} URLs")
        
        # Check if job exists in the database
        existing_job = await get_job(job_id)
        
        # If job doesn't exist, create it
        if not existing_job:
            logger.info(f"Job {job_id} not found in database. Creating new job record.")
            
            # Create a job with the initial data and the job_id
            job_data = {
                "job_id": job_id,
                "collection_id": collection_id,
                "title": script_data.get("title", "Generated Script"),
                "status": JobStatus.PENDING  # Will be updated to READY after creation
            }
            
            try:
                # The create_job function will return the job_id
                created_job_id = await create_job(job_data)
                logger.info(f"Created new job with ID: {created_job_id}")
                
                # Proceed with update after creating
                await update_job_from_script_ready(job_id, update_data)
                logger.info(f"Updated new job {job_id} with script data")
            except Exception as create_error:
                logger.error(f"Error creating job: {str(create_error)}")
        else:
            # Job exists, update it
            try:
                await update_job_from_script_ready(job_id, update_data)
                logger.info(f"Updated existing job {job_id} with script data")
            except Exception as db_error:
                logger.error(f"Error updating job in database: {str(db_error)}")
        
        # Format data for WebSocket message
        ws_data = {
            "type": "job_complete",
            "job_id": job_id,
            "script_id": job_id,
            "script_text": script_text,
            "audio_url": audio_url,
            "image_urls": image_urls
        }
        
        # Send a WebSocket message to all clients subscribed to this job
        await ws_manager.send_job_update(job_id, ws_data)
        
        # If this script is associated with a collection, notify those subscribers too
        if collection_id:
            logger.info(f"Sending collection update for collection_id: {collection_id}")
            collection_data = {
                "type": "script_generated",
                "collection_id": collection_id,
                "script_id": job_id,
                "status": "completed",
                "progress": 100
            }
            await ws_manager.send_collection_update(collection_id, collection_data)
            
    except Exception as e:
        logger.error(f"Error handling script.ready event: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.http_client = httpx.AsyncClient(
        timeout=TIMEOUT,
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
    )
    # Connect to MongoDB
    await connect_to_mongodb()
    
    # Connect to RabbitMQ and start consuming
    try:
        await message_broker.connect()
        
        # Register the default callback handler for all script.ready events
        message_broker.register_default_callback(handle_script_ready)
        
        # Start consuming script.ready events
        await message_broker.start_consuming_script_ready()
        logger.info("RabbitMQ consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to set up RabbitMQ connection: {str(e)}")
    
    yield
    
    # Shutdown
    await app.state.http_client.aclose()
    await message_broker.close()
    await close_mongodb_connection()

app = FastAPI(
    title="API Gateway",
    description="Gateway service for handling requests to microservices",
    version="1.0.0",
    lifespan=lifespan
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests"""
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response: {response.status_code}")
    return response

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Global error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

async def make_service_request(method: str, url: str, **kwargs) -> httpx.Response:
    """Make a request to a service with retry logic"""
    client = app.state.http_client
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            if attempt == MAX_RETRIES - 1:
                raise HTTPException(status_code=504, detail="Service timeout")
            logger.warning(f"Request timeout, attempt {attempt + 1}/{MAX_RETRIES}")
        except httpx.HTTPStatusError as e:
            if attempt == MAX_RETRIES - 1:
                raise HTTPException(status_code=e.response.status_code, detail=str(e))
            logger.warning(f"Request failed, attempt {attempt + 1}/{MAX_RETRIES}")
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise HTTPException(status_code=500, detail=str(e))
            logger.warning(f"Request error, attempt {attempt + 1}/{MAX_RETRIES}")

@app.post("/api/collections/upload-file")
async def upload_file(
    file: UploadFile = File(...),
    style: str = Form(...),
    target_audience: str = Form(...),
    duration: str = Form(...),
    language: str = Form(...),
    visual_style: str = Form(...),
    voice: str = Form(...),
    style_description: Optional[str] = Form(None),
):
    """Upload a file to the data collector"""
    try:
        # Forward request to data collector
        async with httpx.AsyncClient() as client:
            await file.seek(0)
            
            # Properly format the files parameter
            files = {"file": (file.filename, await file.read(), file.content_type)}
            
            data = {
                "style": style,
                "target_audience": target_audience,
                "duration": duration,
                "language": language,
                "visual_style": visual_style,
                "voice": voice,
            }

            response = await client.post(
                f"{DATA_COLLECTOR_URL}/api/collections/upload-file",
                files=files,
                data=data,
                timeout=60.0
            )

            if response.status_code != 200 and response.status_code != 201:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.json().get("detail", "Error processing file")
                )

            return response.json()

    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/collections/wikipedia")
async def process_wikipedia_url(
    data: Dict[str, str]
):
    """Process a Wikipedia URL and create a collection"""
    url = data.get('url')
    style = data.get('style')
    target_audience = data.get('target_audience')
    duration = data.get('duration')
    voice = data.get('voice')
    language = data.get('language')
    visual_style = data.get('visual_style')
    if not url:
        raise HTTPException(status_code=400, detail="URL is required")
    logger.info(f"Processing Wikipedia URL: {url}")
    logger.info(f"Style: {style}")
    logger.info(f"Target Audience: {target_audience}")
    logger.info(f"Duration: {duration}")
    logger.info(f"Voice: {voice}")
    logger.info(f"Language: {language}")
    logger.info(f"Visual Style: {visual_style}")
    
    try:
        # Use the make_service_request helper which has retry logic
        logger.info(f"Sending Wikipedia URL request to data collector: {url}")
        response = await make_service_request(
            "POST",
            f"{DATA_COLLECTOR_URL}/api/collections/wikipedia",
            json={
                "url": url,
                "style": style,
                "target_audience": target_audience,
                "duration": duration,
                "voice": voice,
                "language": language,
                "visual_style": visual_style
            }
        )
        
        logger.info(f"Wikipedia response successful with status: {response.status_code}")
        return response.json()
    except HTTPException as he:
        # Re-raise HTTP exceptions directly
        logger.error(f"HTTP error processing Wikipedia URL: {str(he)}")
        raise
    except Exception as e:
        # Log and wrap other exceptions
        logger.error(f"Error processing Wikipedia URL: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing Wikipedia URL: {str(e)}")

@app.post("/api/collections")
async def process_script(
    data: Dict[str, Any]
):
    """Process a script and create a collection"""
    # Check required fields
    if 'content' not in data:
        raise HTTPException(status_code=400, detail="Content is required")
    
    # Extract fields from data
    content = data.get('content')
    title = data.get('title', 'User Script')
    metadata = data.get('metadata', {})
    
    try:
        # Prepare request body with all fields
        request_body = {
            "title": title,
            "content": content,
            "script_type": metadata.get('script_type'),
            "target_audience": metadata.get('target_audience'),
            "duration": metadata.get('duration'),
            "voice": metadata.get('voice'),
            "language": metadata.get('language'),
            "visual_style": metadata.get('visual_style')
        }
        
        # Filter out None values
        request_body = {k: v for k, v in request_body.items() if v is not None}
        
        # Use the make_service_request helper which has retry logic
        logger.info(f"Sending script to data collector: {DATA_COLLECTOR_URL}. Title: {title}")
        response = await make_service_request(
            "POST",
            f"{DATA_COLLECTOR_URL}/api/collections/script",
            json=request_body
        )
        
        logger.info(f"Script processing successful with status: {response.status_code}")
        return response.json()
    except HTTPException as he:
        # Re-raise HTTP exceptions directly
        logger.error(f"HTTP error processing script: {str(he)}")
        raise
    except Exception as e:
        # Log and wrap other exceptions
        logger.error(f"Error processing script: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing script: {str(e)}")

@app.get("/api/collections/{collection_id}")
async def get_collection(collection_id: str):
    """
    Get a collection and all its associated jobs by collection_id
    """
    try:
        logger.info(f"Retrieving collection with ID: {collection_id}")
        
        # Access the jobs collection using MONGO_JOBS_COLLECTION from config
        jobs_collection = db[MONGO_DB]
        
        # Find all jobs with the given collection_id
        cursor = jobs_collection.find({"collection_id": collection_id})
        jobs = await cursor.to_list(length=100)
        
        if not jobs:
            logger.warning(f"No jobs found for collection_id: {collection_id}")
            raise HTTPException(status_code=404, detail=f"Collection {collection_id} not found")
        
        # Transform jobs for frontend
        collection_jobs = []
        for job in jobs:
            # Convert ObjectId to string
            if "_id" in job:
                job["_id"] = str(job["_id"])
            
            # Ensure job_id is present
            if "job_id" not in job and "_id" in job:
                job["job_id"] = job["_id"]
                
            collection_jobs.append(job)
        
        logger.info(f"Found {len(collection_jobs)} jobs for collection_id: {collection_id}")
        
        # Return the collection data including all jobs
        return {
            "collection_id": collection_id,
            "jobs": collection_jobs,
            "total_jobs": len(collection_jobs)
        }
        
    except Exception as e:
        error_msg = f"Error retrieving collection {collection_id}: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@app.get("/api/scripts/{script_id}/status")
async def get_script_status(script_id: str):
    """Get the status of a script generation job"""
    try:
        job = await get_job(script_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "job_id": job["id"],
            "status": job["status"],
            "created_at": job["created_at"],
            "updated_at": job["updated_at"]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting script status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/scripts/{script_id}")
async def get_script(script_id: str):
    """Get a script by ID"""
    try:
        job = await get_job(script_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
            
        # Only return the job status if it's not ready
        if job["status"] != JobStatus.READY:
            return {
                "job_id": job["id"],
                "status": job["status"],
                "created_at": job["created_at"],
                "updated_at": job["updated_at"]
            }
        
        # Format script data for the frontend in the expected structure
        result = {
            "job_id": job.get("id") or job.get("job_id"),
            "status": job.get("status", JobStatus.PENDING),
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at")
        }
        
        # Extract script data
        script_data = {}
        
        # If job has structured script data
        if "script" in job and isinstance(job["script"], dict):
            script_data = job["script"]
        else:
            # Try to construct script from other fields
            scenes = []
            
            # Check if we have script_text, which might be the entire script
            script_text = job.get("script_text")
            if script_text:
                # Create a single scene with the entire script text
                scenes.append({
                    "scene_id": f"{script_id}_scene_1",
                    "script": script_text,
                    "visual": job.get("visual_description", "No visual description available")
                })
                
            # Add metadata if available
            metadata = {
                "title": job.get("title", "Generated Script"),
                "style": job.get("style"),
                "target_audience": job.get("target_audience"),
                "duration": job.get("duration")
            }
            
            # Filter out None values
            metadata = {k: v for k, v in metadata.items() if v is not None}
            
            script_data = {
                "_id": script_id,
                "scenes": scenes,
                "metadata": metadata
            }
        
        # Add script data to result
        result["script"] = script_data
        
        # Add voice data if available
        voice_data = job.get("voice_data")
        audio_url = job.get("audio_url")
        
        if voice_data:
            result["voice"] = voice_data
        elif audio_url:
            # Construct basic voice data structure
            scene_voiceovers = []
            
            # If the job has structured scene data
            if script_data.get("scenes"):
                for i, scene in enumerate(script_data["scenes"]):
                    scene_id = scene.get("scene_id") or f"{script_id}_scene_{i+1}"
                    
                    # If multiple audio URLs are provided as a list
                    if isinstance(audio_url, list) and i < len(audio_url):
                        scene_audio_url = audio_url[i]
                    else:
                        scene_audio_url = audio_url
                        
                    scene_voiceovers.append({
                        "scene_id": scene_id,
                        "voice_id": job.get("voice", "default"),
                        "audio_url": scene_audio_url,
                        "cloudinary_url": scene_audio_url,
                        "duration": job.get("audio_duration", 30)  # Default duration in seconds
                    })
            else:
                # Create a single voiceover for the entire script
                scene_voiceovers.append({
                    "scene_id": f"{script_id}_scene_1",
                    "voice_id": job.get("voice", "default"),
                    "audio_url": audio_url,
                    "cloudinary_url": audio_url,
                    "duration": job.get("audio_duration", 30)  # Default duration in seconds
                })
                
            result["voice"] = {
                "script_id": script_id,
                "collection_id": job.get("collection_id", ""),
                "scene_voiceovers": scene_voiceovers
            }
            
        # Add image data if available
        image_data = job.get("image_data")
        image_urls = job.get("image_urls")
        
        if image_data:
            result["image"] = image_data
        elif image_urls:
            # Construct basic image data structure
            scene_images = []
            
            # If the job has structured scene data
            if script_data.get("scenes"):
                for i, scene in enumerate(script_data["scenes"]):
                    scene_id = scene.get("scene_id") or f"{script_id}_scene_{i+1}"
                    
                    # If multiple image URLs are provided as a list
                    if isinstance(image_urls, list) and i < len(image_urls):
                        scene_image_url = image_urls[i]
                        
                        # Handle case where image_urls might contain objects
                        if isinstance(scene_image_url, dict) and "url" in scene_image_url:
                            scene_image_url = scene_image_url["url"]
                    else:
                        # Use the single image URL for all scenes or default to first in list
                        scene_image_url = image_urls[0] if isinstance(image_urls, list) and image_urls else image_urls
                        
                    scene_images.append({
                        "scene_id": scene_id,
                        "cloudinary_url": scene_image_url
                    })
            else:
                # Create a single image for the entire script
                image_url = image_urls[0] if isinstance(image_urls, list) and image_urls else image_urls
                scene_images.append({
                    "scene_id": f"{script_id}_scene_1",
                    "cloudinary_url": image_url
                })
                
            result["image"] = {
                "script_id": script_id,
                "collection_id": job.get("collection_id", ""),
                "scene_images": scene_images
            }
        
        # Log what we're returning
        logger.info(f"Returning script data for ID: {script_id}")
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting script: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, job_id: Optional[str] = Query(None), collection_id: Optional[str] = Query(None)):
    """WebSocket endpoint for real-time updates"""
    try:
        # Accept the WebSocket connection
        await ws_manager.connect(websocket, job_id, collection_id)
        
        try:
            # If job_id is provided, send current job status
            if job_id:
                job = await get_job(job_id)
                if job:
                    await websocket.send_json({
                        "type": "job_status",
                        "job_id": job["id"],
                        "status": job["status"]
                    })
                    
                    # If the job is already complete, send the complete data
                    if job["status"] == JobStatus.READY:
                        await websocket.send_json({
                            "type": "job_complete",
                            "job_id": job["id"],
                            "script_text": job["script_text"],
                            "audio_url": job["audio_url"],
                            "image_urls": job["image_urls"]
                        })
            
            # If collection_id is provided, send current collection status
            elif collection_id:
                # Check if this collection has associated scripts
                # Use the proper db variable from database.py, not db_client
                from database import db
                
                # Check if the db connection is established
                if db is None:
                    logger.error("Database connection not established")
                    await websocket.send_json({
                        "type": "error",
                        "message": "Database connection not established"
                    })
                    return
                
                try:
                    scripts_collection = db.scripts
                    collection_scripts = await scripts_collection.find({"collection_id": collection_id}).to_list(1)
                    
                    if collection_scripts and len(collection_scripts) > 0:
                        # Collection already has scripts
                        script = collection_scripts[0]
                        await websocket.send_json({
                            "type": "collection_status",
                            "collection_id": collection_id,
                            "status": "processed",
                            "script_id": str(script["_id"]),
                            "progress": 100
                        })
                    else:
                        # Collection does not have scripts yet
                        await websocket.send_json({
                            "type": "collection_status",
                            "collection_id": collection_id,
                            "status": "waiting",
                            "progress": 0
                        })
                except Exception as e:
                    logger.error(f"Error checking collection scripts: {str(e)}")
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Error checking collection status: {str(e)}"
                    })
            
            # Listen for messages (ping/pong or commands)
            while True:
                data = await websocket.receive_text()
                # Handle ping/pong for heartbeat
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
                # Handle JSON messages
                else:
                    try:
                        json_data = json.loads(data)
                        if json_data.get("type") == "ping":
                            await websocket.send_json({"type": "pong"})
                    except json.JSONDecodeError:
                        # Not a valid JSON message, ignore
                        pass
                
        except WebSocketDisconnect:
            ws_manager.disconnect(websocket, job_id, collection_id)
        
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
        # Attempt to close
        try:
            await websocket.close()
        except:
            pass

@app.get("/api/configurations/styles", response_model=List[StyleConfiguration])
async def get_styles():
    """Get available styles"""
    styles = await get_configurations("styles")
    return styles

@app.get("/api/configurations/languages", response_model=List[Configuration])
async def get_languages():
    """Get available languages"""
    languages = await get_configurations("languages")
    return languages

@app.get("/api/configurations/voices", response_model=List[VoiceConfiguration])
async def get_voices():
    """Get available voices"""
    voices = await get_configurations("voices")
    return voices

@app.get("/api/configurations/visual-styles", response_model=List[Configuration])
async def get_visual_styles():
    """Get available visual styles"""
    visual_styles = await get_configurations("visual_styles")
    return visual_styles

@app.get("/api/configurations/target-audiences", response_model=List[TargetAudienceConfiguration])
async def get_target_audiences():
    """Get available target audiences"""
    target_audiences = await get_configurations("target_audience")
    return target_audiences

@app.get("/api/configurations/durations", response_model=List[DurationConfiguration])
async def get_durations():
    """Get available durations"""
    durations = await get_configurations("durations")
    return durations

@app.post("/api/v1/voice/synthesize")
async def synthesize_voice(request: Request):
    """Forward voice synthesis request to the voice synthesis service"""
    try:
        # Get the request body
        body = await request.json()
        logger.info(f"Forwarding voice synthesis request: {json.dumps(body, default=str)[:200]}...")
        
        # Forward to voice synthesis service
        response = await make_service_request(
            "POST",
            f"{VOICE_SYNTHESIS_URL}/api/v1/voice/synthesize",
            json=body
        )
        
        logger.info(f"Voice synthesis request successful with status: {response.status_code}")
        return response.json()
    except HTTPException as he:
        # Re-raise HTTP exceptions directly
        logger.error(f"HTTP error in voice synthesis: {str(he)}")
        raise
    except Exception as e:
        # Log and wrap other exceptions
        logger.error(f"Error in voice synthesis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error in voice synthesis: {str(e)}")

@app.post("/api/visuals")
async def generate_visuals(request: Request):
    """Forward visual generation request to the visual generation service"""
    try:
        # Get the request body
        body = await request.json()
        logger.info(f"Forwarding visual generation request: {json.dumps(body, default=str)[:200]}...")
        
        # Forward to visual generation service
        response = await make_service_request(
            "POST",
            f"{VISUAL_GENERATION_URL}/api/visuals",
            json=body
        )
        
        logger.info(f"Visual generation request successful with status: {response.status_code}")
        return response.json()
    except HTTPException as he:
        # Re-raise HTTP exceptions directly
        logger.error(f"HTTP error in visual generation: {str(he)}")
        raise
    except Exception as e:
        # Log and wrap other exceptions
        logger.error(f"Error in visual generation: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error in visual generation: {str(e)}")


# Handler for Vercel serverless function
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 