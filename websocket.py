import logging
import json
from typing import Dict, List, Any, Optional
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger("api_gateway.websocket")

class ConnectionManager:
    def __init__(self):
        # Map of job_id/collection_id to list of connected WebSockets
        self.active_connections: Dict[str, List[WebSocket]] = {}
        
    async def connect(self, websocket: WebSocket, job_id: Optional[str] = None, collection_id: Optional[str] = None):
        """Connect a WebSocket client with either job_id or collection_id"""
        await websocket.accept()
        
        connection_key = None
        
        # Prioritize job_id if both are provided
        if job_id:
            connection_key = f"job:{job_id}"
            logger.info(f"WebSocket connected for job_id: {job_id}")
        elif collection_id:
            connection_key = f"collection:{collection_id}"
            logger.info(f"WebSocket connected for collection_id: {collection_id}")
        else:
            connection_key = "general"
            logger.info("WebSocket connected without id")
            
        # Store the connection
        if connection_key not in self.active_connections:
            self.active_connections[connection_key] = []
        self.active_connections[connection_key].append(websocket)
    
    def disconnect(self, websocket: WebSocket, job_id: Optional[str] = None, collection_id: Optional[str] = None):
        """Disconnect a WebSocket client"""
        connection_key = None
        
        if job_id:
            connection_key = f"job:{job_id}"
        elif collection_id:
            connection_key = f"collection:{collection_id}" 
        else:
            connection_key = "general"
        
        if connection_key in self.active_connections:
            try:
                self.active_connections[connection_key].remove(websocket)
                logger.info(f"WebSocket disconnected from {connection_key}")
                
                # Clean up empty connection lists
                if not self.active_connections[connection_key]:
                    del self.active_connections[connection_key]
            except ValueError:
                pass  # WebSocket wasn't in the list
    
    async def send_job_update(self, job_id: str, message: Dict[str, Any]):
        """Send an update to all clients subscribed to a specific job_id"""
        connection_key = f"job:{job_id}"
        await self._send_update(connection_key, message)
    
    async def send_collection_update(self, collection_id: str, message: Dict[str, Any]):
        """Send an update to all clients subscribed to a specific collection_id"""
        connection_key = f"collection:{collection_id}"
        await self._send_update(connection_key, message)
    
    async def _send_update(self, connection_key: str, message: Dict[str, Any]):
        """Internal method to send updates to connections identified by a key"""
        if connection_key in self.active_connections:
            disconnected = []
            
            for i, websocket in enumerate(self.active_connections[connection_key]):
                try:
                    await websocket.send_json(message)
                    logger.info(f"Sent update for {connection_key}")
                except WebSocketDisconnect:
                    disconnected.append(i)
                except Exception as e:
                    logger.error(f"Error sending update to WebSocket: {str(e)}")
                    disconnected.append(i)
            
            # Remove disconnected sockets (in reverse to preserve indices)
            for i in sorted(disconnected, reverse=True):
                self.active_connections[connection_key].pop(i)
            
            # Clean up the entry if no connections remain
            if not self.active_connections[connection_key]:
                del self.active_connections[connection_key]
                logger.info(f"Removed empty connection list for {connection_key}")
        else:
            logger.warning(f"No active connections for {connection_key}")
    
    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast a message to all connected clients"""
        for connection_key in list(self.active_connections.keys()):
            disconnected = []
            
            for i, websocket in enumerate(self.active_connections[connection_key]):
                try:
                    await websocket.send_json(message)
                except WebSocketDisconnect:
                    disconnected.append(i)
                except Exception as e:
                    logger.error(f"Error broadcasting message: {str(e)}")
                    disconnected.append(i)
            
            # Remove disconnected sockets (in reverse to preserve indices)
            for i in sorted(disconnected, reverse=True):
                self.active_connections[connection_key].pop(i)
            
            # Clean up the entry if no connections remain
            if not self.active_connections[connection_key]:
                del self.active_connections[connection_key] 