import aio_pika
import json
import logging
from typing import Dict, Any, Optional, Callable
from config import (
    RABBITMQ_URL,
    SCRIPT_EVENTS_EXCHANGE,
    SCRIPT_READY_ROUTING_KEY,
    SCRIPT_EVENTS_QUEUE
)

logger = logging.getLogger("api_gateway.broker")

class MessageBroker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None
        self.script_ready_queue = None
        self.callbacks = {}  # Store WebSocket callbacks keyed by job_id
        self.default_callback = None  # Default callback for handling messages without registered callbacks
        
    async def connect(self):
        """Connect to RabbitMQ and set up exchanges and queues"""
        try:
            logger.info(f"Connecting to RabbitMQ at {RABBITMQ_URL.split('@')[-1]}")
            
            # Connect and create channel
            self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
            self.channel = await self.connection.channel()
            
            # Declare exchange for script events
            self.exchange = await self.channel.declare_exchange(
                SCRIPT_EVENTS_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            # Declare and bind queue for script.ready events
            self.script_ready_queue = await self.channel.declare_queue(
                SCRIPT_EVENTS_QUEUE,
                durable=True
            )
            
            await self.script_ready_queue.bind(
                self.exchange,
                routing_key=SCRIPT_READY_ROUTING_KEY
            )
            
            logger.info("RabbitMQ connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise
    
    
    async def start_consuming_script_ready(self):
        """Start consuming script.ready events"""
        if not self.script_ready_queue:
            logger.error("Cannot consume: Not connected to RabbitMQ")
            return
            
        async def process_message(message):
            async with message.process():
                try:
                    body = json.loads(message.body.decode())
                    logger.info(f"Received message: {body}")
                    
                    # Try to extract job_id from different possible formats in the message
                    job_id = None
                    script_data = body.get("script", {})
                    
                    # Check for _id in script
                    if isinstance(script_data, dict):
                        job_id = script_data.get("_id")
                    
                    # If not found, try script_id
                    if not job_id and isinstance(body, dict):
                        job_id = body.get("script_id")
                    
                    # If still not found, try collection_id
                    if not job_id and isinstance(body, dict):
                        job_id = body.get("collection_id")
                    
                    if not job_id:
                        logger.warning(f"Received script.ready event without identifiable job_id: {body}")
                        # Use default callback if available
                        if self.default_callback:
                            await self.default_callback(body)
                        return
                        
                    logger.info(f"Received script.ready event for job_id: {job_id}")
                    
                    # Look for a callback with exact match
                    callback = self.callbacks.get(job_id)
                    
                    # If not found, try to find a callback with a partial match
                    if not callback and isinstance(job_id, str):
                        for registered_id, cb in self.callbacks.items():
                            if isinstance(registered_id, str) and (registered_id in job_id or job_id in registered_id):
                                logger.info(f"Found partial match callback: {registered_id} for job_id: {job_id}")
                                callback = cb
                                break
                    
                    # Call the registered callback if found
                    if callback:
                        await callback(body)
                        logger.info(f"Callback executed for job_id: {job_id}")
                    elif self.default_callback:
                        # Use default callback if available and no specific callback found
                        logger.info(f"Using default callback for job_id: {job_id}")
                        await self.default_callback(body)
                    else:
                        logger.warning(f"No callback registered for job_id: {job_id} and no default callback")
                        
                except json.JSONDecodeError:
                    logger.error("Failed to decode JSON from message")
                except Exception as e:
                    logger.error(f"Error processing script.ready event: {str(e)}")
        
        await self.script_ready_queue.consume(process_message)
        logger.info(f"Started consuming script.ready events from queue: {self.script_ready_queue.name}")
    
    def register_callback(self, job_id: str, callback: Callable):
        """Register a callback for a specific job_id"""
        self.callbacks[job_id] = callback
        logger.info(f"Registered callback for job_id: {job_id}")
        
    def register_default_callback(self, callback: Callable):
        """Register a default callback for messages without specific handlers"""
        self.default_callback = callback
        logger.info(f"Registered default callback")
        
    def unregister_callback(self, job_id: str):
        """Unregister a callback for a specific job_id"""
        if job_id in self.callbacks:
            del self.callbacks[job_id]
            logger.info(f"Unregistered callback for job_id: {job_id}")
    
    async def close(self):
        """Close the RabbitMQ connection"""
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed") 