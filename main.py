from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import logging
from typing import Dict, Any, List, Optional
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from database import connect_to_mongodb, close_mongodb_connection, get_configurations
from config import (
    DATA_COLLECTOR_URL,
    SCRIPT_GENERATOR_URL,
    MONGO_URI,
    TIMEOUT,
    MAX_RETRIES,
    CORS_ORIGINS,
    LOG_LEVEL
)

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.http_client = httpx.AsyncClient(
        timeout=TIMEOUT,
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
    )
    # Connect to MongoDB
    await connect_to_mongodb()
    yield
    # Shutdown
    await app.state.http_client.aclose()
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
    script_type: str = Form(...),
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
                "script_type": script_type,
                "target_audience": target_audience,
                "duration": duration,
                "language": language,
                "visual_style": visual_style,
                "voice": voice,
            }

            response = await client.post(
                f"{DATA_COLLECTOR_URL}/api/collections/upload-file",
                files=files,
                data=data
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
    script_type = data.get('script_type')
    target_audience = data.get('target_audience')
    duration = data.get('duration')
    voice = data.get('voice')
    language = data.get('language')
    visual_style = data.get('visual_style')
    if not url:
        raise HTTPException(status_code=400, detail="URL is required")
    
    try:
        # Use the make_service_request helper which has retry logic
        logger.info(f"Sending Wikipedia URL request to data collector: {url}")
        response = await make_service_request(
            "POST",
            f"{DATA_COLLECTOR_URL}/api/collections/wikipedia",
            json={
                "url": url,
                "script_type": script_type,
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
        logger.info(f"Sending script to data collector. Title: {title}")
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
    """Get a collection by ID"""
    try:
        # Use the make_service_request helper which has retry logic
        logger.info(f"Getting collection with ID: {collection_id}")
        response = await make_service_request(
            "GET",
            f"{DATA_COLLECTOR_URL}/api/collections/{collection_id}"
        )
        
        logger.info(f"Collection retrieval successful with status: {response.status_code}")
        return response.json()
    except HTTPException as he:
        # Re-raise HTTP exceptions directly
        logger.error(f"HTTP error retrieving collection: {str(he)}")
        raise
    except Exception as e:
        # Log and wrap other exceptions
        logger.error(f"Error retrieving collection: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving collection: {str(e)}")

# Script Generator Routes
@app.post("/api/scripts")
async def create_script(request: Request):
    """Route script creation requests to script generator with style configurations"""
    try:
        data = await request.json()
        # Validate required fields
        required_fields = ['content', 'script_type', 'language', 'voice_id', 'style_description']
        for field in required_fields:
            if field not in data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Transform the request data to match script generator's expected format
        transformed_data = {
            'content': data['content'],
            'script_type': data['script_type'],
            'target_audience': data.get('target_audience', 'general'),
            'duration_seconds': data.get('duration_seconds', 300),
            'tone': data.get('tone', 'informative'),
            'style_description': data['style_description'],
            'language': data['language'],
            'voice_id': data['voice_id'],
            'collection_id': data.get('collection_id')
        }
        
        # Print request content for debugging
        logger.info(f"Script creation request content: {transformed_data}")
        response = await make_service_request(
            "POST",
            f"{SCRIPT_GENERATOR_URL}/api/v1/scripts",
            json=transformed_data
        )
        return JSONResponse(
            status_code=response.status_code,
            content=response.json()
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error creating script: {str(e)}")
        raise HTTPException(status_code=500, detail="Error creating script")

@app.get("/api/scripts/{script_id}/status")
async def get_script_status(script_id: str):
    """Route script status requests to script generator"""
    try:
        response = await make_service_request("GET", f"{SCRIPT_GENERATOR_URL}/api/v1/scripts/{script_id}/status")
        return JSONResponse(
            status_code=response.status_code,
            content=response.json()
        )
    except Exception as e:
        logger.error(f"Error getting script status: {str(e)}")
        raise HTTPException(status_code=500, detail="Error getting script status")

@app.get("/api/scripts/{script_id}")
async def get_script(script_id: str):
    """Route script retrieval requests to script generator"""
    try:
        response = await make_service_request("GET", f"{SCRIPT_GENERATOR_URL}/api/v1/scripts/{script_id}")
        return JSONResponse(
            status_code=response.status_code,
            content=response.json()
        )
    except Exception as e:
        logger.error(f"Error retrieving script: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving script")

# Configuration endpoints
@app.get("/api/configurations/styles", response_model=List[StyleConfiguration])
async def get_styles():
    """Get available style configurations"""
    try:
        return await get_configurations("styles")
    except Exception as e:
        logger.error(f"Error fetching styles: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching styles")

@app.get("/api/configurations/languages", response_model=List[Configuration])
async def get_languages():
    """Get available language configurations"""
    try:
        return await get_configurations("languages")
    except Exception as e:
        logger.error(f"Error fetching languages: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching languages")

@app.get("/api/configurations/voices", response_model=List[VoiceConfiguration])
async def get_voices():
    """Get available voice configurations"""
    try:
        return await get_configurations("voices")
    except Exception as e:
        logger.error(f"Error fetching voices: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching voices")

@app.get("/api/configurations/visual-styles", response_model=List[Configuration])
async def get_visual_styles():
    """Get available visual style configurations"""
    try:
        return await get_configurations("visual_styles")
    except Exception as e:
        logger.error(f"Error fetching visual styles: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching visual styles")

@app.get("/api/configurations/target-audiences", response_model=List[TargetAudienceConfiguration])
async def get_target_audiences():
    """Get available target audience configurations"""
    try:
        return await get_configurations("target_audience")
    except Exception as e:
        logger.error(f"Error fetching target audiences: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching target audiences")

@app.get("/api/configurations/durations", response_model=List[DurationConfiguration])
async def get_durations():
    """Get available duration configurations"""
    try:
        return await get_configurations("durations")
    except Exception as e:
        logger.error(f"Error fetching durations: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching durations")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 