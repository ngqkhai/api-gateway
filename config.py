import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Service URLs
DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")
SCRIPT_GENERATOR_URL = os.getenv("SCRIPT_GENERATOR_URL", "http://localhost:8002")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "api_gateway")

# HTTP Client Configuration
TIMEOUT = 30.0  # seconds
MAX_RETRIES = 3

# CORS Configuration
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO") 