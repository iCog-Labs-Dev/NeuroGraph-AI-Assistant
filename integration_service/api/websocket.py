"""WebSocket endpoint for real-time mining progress updates."""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import Dict, Set
import json
import asyncio
from ..services.progress_watcher import start_watching

router = APIRouter()

# Store active WebSocket connections per job_id
active_connections: Dict[str, Set[WebSocket]] = {}


class ConnectionManager:
    """Manage WebSocket connections for real-time updates."""
    
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, job_id: str):
        """Accept and register a new WebSocket connection."""
        await websocket.accept()
        if job_id not in self.active_connections:
            self.active_connections[job_id] = set()
        self.active_connections[job_id].add(websocket)
        print(f"[WebSocket] Client connected for job {job_id}. Total: {len(self.active_connections[job_id])}", flush=True)
    
    def disconnect(self, websocket: WebSocket, job_id: str):
        """Remove a WebSocket connection."""
        if job_id in self.active_connections:
            self.active_connections[job_id].discard(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]
        print(f"[WebSocket] Client disconnected from job {job_id}", flush=True)
    
    async def broadcast(self, job_id: str, message: dict):
        """Send message to all connections for a specific job."""
        if job_id not in self.active_connections:
            return
        
        # Create a copy to avoid modification during iteration
        connections = list(self.active_connections[job_id])
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"[WebSocket] Failed to send to client: {e}", flush=True)
                self.disconnect(connection, job_id)


# Global connection manager
manager = ConnectionManager()


@router.websocket("/ws/mining-progress/{job_id}")
async def websocket_mining_progress(websocket: WebSocket, job_id: str):
    """
    WebSocket endpoint for real-time mining progress updates.
    Clients connect with job_id to receive live updates.
    """
    await manager.connect(websocket, job_id)
    
    # Start watching progress file for this job
    await start_watching(job_id, manager)
    
    try:
        # Send initial connection confirmation
        await websocket.send_json({
            "status": "connected",
            "progress": 0,
            "message": "Connected to progress stream"
        })
        
        # Keep connection alive and handle incoming messages (if any)
        while True:
            # Wait for any client messages (ping/pong for keep-alive)
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                # Echo back to confirm connection is alive
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                # Send keep-alive ping
                try:
                    await websocket.send_json({
                        "type": "keep-alive",
                        "message": "Connection alive"
                    })
                except:
                    break
                    
    except WebSocketDisconnect:
        manager.disconnect(websocket, job_id)
    except Exception as e:
        print(f"[WebSocket] Error: {e}", flush=True)
        manager.disconnect(websocket, job_id)
