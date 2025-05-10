from http.server import BaseHTTPRequestHandler
from main import app
import json

def handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'API Gateway Service'})
    } 