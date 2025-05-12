import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Service URLs
DATA_COLLECTOR_URL = os.getenv("DATA_COLLECTOR_URL", "http://localhost:8001")
SCRIPT_GENERATOR_URL = os.getenv("SCRIPT_GENERATOR_URL", "http://localhost:8002")
VOICE_SYNTHESIS_URL = os.getenv("VOICE_SYNTHESIS_URL", "http://localhost:8003")
VISUAL_GENERATION_URL = os.getenv("VISUAL_GENERATION_URL", "http://localhost:8004")
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8005")

# RabbitMQ Configuration
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
SCRIPT_EVENTS_EXCHANGE = os.getenv("SCRIPT_EVENTS_EXCHANGE", "script_events")
SCRIPT_EVENTS_QUEUE = os.getenv("SCRIPT_EVENTS_QUEUE", "script_events")
SCRIPT_READY_ROUTING_KEY = os.getenv("SCRIPT_READY_ROUTING_KEY", "script.ready")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "api_gateway")

# HTTP Client Configuration
TIMEOUT = 30.0  # seconds
MAX_RETRIES = 3

# CORS Configuration
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO") 