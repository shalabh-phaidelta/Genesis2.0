from typing import List

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connected = False

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print("connected to websocket")
        self.connected = True

    async def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        self.connected = False
        print("disconnected to websocket")