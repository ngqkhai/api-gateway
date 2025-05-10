from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from main import app
import json
import asyncio
from database import connect_to_mongodb, close_mongodb_connection

def handler(event, context):
    """Handle serverless function requests"""
    # Convert Vercel event to ASGI scope
    path = event.get('path', '/')
    method = event.get('method', 'GET')
    headers = event.get('headers', {})
    query_string = event.get('queryStringParameters', {})
    body = event.get('body', '')
    
    # Create ASGI scope
    scope = {
        'type': 'http',
        'method': method,
        'scheme': 'https',
        'server': ('vercel', 443),
        'path': path,
        'query_string': query_string.encode() if query_string else b'',
        'headers': [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        'raw_path': path.encode(),
    }
    
    # Create ASGI message
    async def receive():
        return {'type': 'http.request', 'body': body.encode() if body else b''}
    
    async def send(message):
        if message['type'] == 'http.response.start':
            context.statusCode = message['status']
            context.headers = dict(message['headers'])
        elif message['type'] == 'http.response.body':
            context.body = message['body']
    
    # Initialize database connection
    async def init_db():
        await connect_to_mongodb()
    
    # Close database connection
    async def cleanup_db():
        await close_mongodb_connection()
    
    # Run the ASGI application with database lifecycle
    async def run_app():
        await init_db()
        try:
            await app(scope, receive, send)
        finally:
            await cleanup_db()
    
    asyncio.run(run_app())
    
    return {
        'statusCode': context.statusCode,
        'headers': context.headers,
        'body': context.body.decode() if isinstance(context.body, bytes) else context.body
    } 