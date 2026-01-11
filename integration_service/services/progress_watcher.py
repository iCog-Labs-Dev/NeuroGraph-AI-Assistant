"""Progress watcher service for broadcasting mining progress via WebSocket."""
import os
import json
import asyncio
from typing import Optional


class ProgressWatcher:
    """Watch progress file and broadcast updates via WebSocket."""
    
    def __init__(self):
        self.watchers = {}
    
    async def watch_progress(self, job_id: str, websocket_manager):
        """
        Watch progress file for a job and broadcast updates to connected clients.
        Stops when progress reaches 100 or file indicates completion.
        """
        progress_path = f"/shared/output/{job_id}/progress.json"
        last_progress = -1
        
        print(f"[ProgressWatcher] Starting watch for job {job_id}", flush=True)
        
        try:
            while True:
                # Check if progress file exists
                if os.path.exists(progress_path):
                    try:
                        with open(progress_path, 'r') as f:
                            data = json.load(f)
                        
                        current_progress = data.get('progress', 0)
                        
                        # Only broadcast if progress changed
                        if current_progress != last_progress:
                            await websocket_manager.broadcast(job_id, data)
                            last_progress = current_progress
                            
                        # Stop watching if completed
                        if data.get('status') == 'completed' or current_progress >= 100:
                            print(f"[ProgressWatcher] Job {job_id} completed", flush=True)
                            break
                            
                    except json.JSONDecodeError:
                        pass  # File being written, skip this iteration
                    except Exception as e:
                        print(f"[ProgressWatcher] Error reading progress: {e}", flush=True)
                
                # Wait before next check
                await asyncio.sleep(0.5)  # Check every 500ms for responsive updates
                
        except asyncio.CancelledError:
            print(f"[ProgressWatcher] Watch cancelled for job {job_id}", flush=True)
        finally:
            # Clean up watcher reference
            if job_id in self.watchers:
                del self.watchers[job_id]


# Global progress watcher instance
progress_watcher = ProgressWatcher()


async def start_watching(job_id: str, manager):
    """Start watching progress for a job."""
    if job_id not in progress_watcher.watchers:
        task = asyncio.create_task(progress_watcher.watch_progress(job_id, manager))
        progress_watcher.watchers[job_id] = task
