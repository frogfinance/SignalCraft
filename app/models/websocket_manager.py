from fastapi import WebSocket
from typing import List
import logging

logger = logging.getLogger("app")

class WebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        """Accepts a WebSocket connection and stores it."""
        await websocket.accept()
        self.active_connections.append(websocket)

    async def disconnect(self, websocket: WebSocket):
        """Removes a WebSocket connection when it disconnects."""
        self.active_connections.remove(websocket)

    async def send_message(self, message: dict):
        """Sends a message to all active WebSocket connections."""
        logger.debug('Sending message to all active connections')
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.exception("Error sending message", exc_info=e)
