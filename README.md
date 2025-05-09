# API Gateway Service

This service acts as a gateway between the frontend application and the microservices (Data Collector and Script Generator).

## Features

- Routes requests to appropriate microservices
- Handles file uploads
- Manages configuration endpoints
- Provides error handling and logging
- Implements CORS protection

## Prerequisites

- Python 3.8+
- Virtual environment (venv)

## Setup

1. Create and activate virtual environment:
```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create `.env` file:
```bash
cp .env.example .env
```

4. Update `.env` with your configuration:
```env
PORT=8000
HOST=0.0.0.0
DATA_COLLECTOR_URL=http://localhost:8001
SCRIPT_GENERATOR_URL=http://localhost:8002
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:8000
LOG_LEVEL=INFO
```

## Running the Service

```bash
python main.py
```

The service will start on http://localhost:8000 by default.

## API Endpoints

### Data Collection
- `POST /api/collections/upload-file` - Upload script file
- `POST /api/collections/wikipedia` - Process Wikipedia article
- `POST /api/collections/script` - Submit script
- `GET /api/collections` - Get all collections
- `GET /api/collections/{collection_id}` - Get specific collection

### Script Generation
- `POST /api/scripts` - Create new script
- `GET /api/scripts/{script_id}/status` - Get script status
- `GET /api/scripts/{script_id}` - Get script details

### Configurations
- `GET /api/configurations/styles` - Get available styles
- `GET /api/configurations/languages` - Get available languages
- `GET /api/configurations/voices` - Get available voices
- `GET /api/configurations/visual-styles` - Get available visual styles

## Error Handling

The service implements comprehensive error handling:
- Input validation
- Service availability checks
- Proper error responses
- Detailed logging

## Logging

Logs are written to stdout with the following levels:
- INFO: Normal operation
- ERROR: Error conditions
- DEBUG: Detailed debugging information (when enabled)

## Development

To run in development mode with auto-reload:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
``` 