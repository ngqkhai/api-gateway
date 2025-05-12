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
        logger.info("MessageBroker initialized")
        
    async def connect(self):
        """Connect to RabbitMQ and set up exchanges and queues"""
        try:
            logger.info(f"Connecting to RabbitMQ at {RABBITMQ_URL.split('@')[-1]}")
            
            # Connect and create channel
            self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
            logger.info(f"RabbitMQ connection established with status: {self.connection.is_closed}")
            
            self.channel = await self.connection.channel()
            logger.info(f"RabbitMQ channel created with ID: {id(self.channel)}")
            
            # Declare exchange for script events
            self.exchange = await self.channel.declare_exchange(
                SCRIPT_EVENTS_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            logger.info(f"Exchange '{SCRIPT_EVENTS_EXCHANGE}' declared with type: {aio_pika.ExchangeType.TOPIC}")
            
            # Declare and bind queue for script.ready events
            self.script_ready_queue = await self.channel.declare_queue(
                SCRIPT_EVENTS_QUEUE,
                durable=True
            )
            logger.info(f"Queue '{SCRIPT_EVENTS_QUEUE}' declared with name: {self.script_ready_queue.name}")
            
            await self.script_ready_queue.bind(
                self.exchange,
                routing_key=SCRIPT_READY_ROUTING_KEY
            )
            logger.info(f"Queue bound to exchange with routing key: {SCRIPT_READY_ROUTING_KEY}")
            
            logger.info("RabbitMQ connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}", exc_info=True)
            raise
    
    
    async def start_consuming_script_ready(self):
        """Start consuming script.ready events"""
        if not self.script_ready_queue:
            logger.error("Cannot consume: Not connected to RabbitMQ")
            return
            
        async def process_message(message):
            async with message.process():
                try:
                    logger.info(f"Processing message with delivery tag: {message.delivery_tag}")
                    body = json.loads(message.body.decode())
                    logger.info(f"Received message body: {json.dumps(body, default=str)[:500]}...")
                    
                    # Try to extract job_id from different possible formats in the message
                    job_id = None
                    script_data = body.get("script", {})
                    
                    # Check for _id in script
                    if isinstance(script_data, dict):
                        job_id = script_data.get("_id")
                        logger.info(f"Extracted job_id from script._id: {job_id}")
                    
                    # If not found, try script_id
                    if not job_id and isinstance(body, dict):
                        job_id = body.get("script_id")
                        if job_id:
                            logger.info(f"Extracted job_id from script_id: {job_id}")
                    
                    # If still not found, try collection_id
                    if not job_id and isinstance(body, dict):
                        job_id = body.get("collection_id")
                        if job_id:
                            logger.info(f"Using collection_id as job_id: {job_id}")
                    
                    if not job_id:
                        logger.warning(f"Received script.ready event without identifiable job_id: {json.dumps(body, default=str)[:200]}...")
                        # Use default callback if available
                        if self.default_callback:
                            logger.info("Calling default callback for message without job_id")
                            await self.default_callback(body)
                        return
                        
                    logger.info(f"Received script.ready event for job_id: {job_id}")
                    
                    # Look for a callback with exact match
                    callback = self.callbacks.get(job_id)
                    if callback:
                        logger.info(f"Found exact match callback for job_id: {job_id}")
                    
                    # If not found, try to find a callback with a partial match
                    if not callback and isinstance(job_id, str):
                        logger.info(f"No exact callback found, searching for partial matches for job_id: {job_id}")
                        for registered_id, cb in self.callbacks.items():
                            if isinstance(registered_id, str) and (registered_id in job_id or job_id in registered_id):
                                logger.info(f"Found partial match callback: {registered_id} for job_id: {job_id}")
                                callback = cb
                                break
                    
                    # Call the registered callback if found
                    if callback:
                        logger.info(f"Executing callback for job_id: {job_id}")
                        await callback(body)
                        logger.info(f"Callback executed successfully for job_id: {job_id}")
                    elif self.default_callback:
                        # Use default callback if available and no specific callback found
                        logger.info(f"No specific callback found. Using default callback for job_id: {job_id}")
                        await self.default_callback(body)
                        logger.info(f"Default callback executed for job_id: {job_id}")
                    else:
                        logger.warning(f"No callback registered for job_id: {job_id} and no default callback")
                        
                except json.JSONDecodeError as json_err:
                    logger.error(f"Failed to decode JSON from message: {str(json_err)}", exc_info=True)
                    logger.error(f"Raw message content: {message.body.decode()[:500]}")
                except Exception as e:
                    logger.error(f"Error processing script.ready event: {str(e)}", exc_info=True)
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
        
        logger.info(f"Starting to consume messages from queue: {self.script_ready_queue.name}")
        await self.script_ready_queue.consume(process_message)
        logger.info(f"Consumer registered for queue: {self.script_ready_queue.name}")
    
    def register_callback(self, job_id: str, callback: Callable):
        """Register a callback for a specific job_id"""
        self.callbacks[job_id] = callback
        logger.info(f"Registered callback for job_id: {job_id}, total callbacks: {len(self.callbacks)}")
        
    def register_default_callback(self, callback: Callable):
        """Register a default callback for messages without specific handlers"""
        self.default_callback = callback
        logger.info(f"Registered default callback: {callback.__name__ if hasattr(callback, '__name__') else 'anonymous'}")
        
    def unregister_callback(self, job_id: str):
        """Unregister a callback for a specific job_id"""
        if job_id in self.callbacks:
            del self.callbacks[job_id]
            logger.info(f"Unregistered callback for job_id: {job_id}, remaining callbacks: {len(self.callbacks)}")
        else:
            logger.warning(f"Attempted to unregister non-existent callback for job_id: {job_id}")
    
    async def close(self):
        """Close the RabbitMQ connection"""
        if self.connection:
            logger.info("Closing RabbitMQ connection...")
            await self.connection.close()
            logger.info("RabbitMQ connection closed successfully")
        else:
            logger.warning("Attempted to close non-existent RabbitMQ connection") 