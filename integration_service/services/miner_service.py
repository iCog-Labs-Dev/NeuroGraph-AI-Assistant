"""Neural Miner communication service."""  
import httpx  
import os  
import asyncio  
from typing import Dict, Any  
from ..config.settings import settings  
  
class MinerService:  
    """Service for communicating with Neural Subgraph Miner."""  
      
    def __init__(self):  
        self.miner_url = settings.miner_url  
        self.timeout = settings.miner_timeout  
      
    async def mine_motifs(self, networkx_file_path: str, max_retries: int = 3) -> Dict[str, Any]:  
        """Send NetworkX file to miner and return discovered motifs."""  
        if not os.path.exists(networkx_file_path):  
            raise FileNotFoundError(f"NetworkX file not found: {networkx_file_path}")  
          
        # Retry logic for network reliability  
        for attempt in range(max_retries):  
            try:  
                # Read NetworkX file  
                with open(networkx_file_path, 'rb') as f:  
                    networkx_data = f.read()  
                  
                # Send to miner using HTTP client  
                async with httpx.AsyncClient(timeout=self.timeout) as client:  
                    files = {'graph_file': ('graph.gpickle', networkx_data, 'application/octet-stream')}  
                    response = await client.post(f"{self.miner_url}/mine", files=files)  
                      
                    if response.status_code != 200:  
                        raise RuntimeError(f"Miner returned {response.status_code}: {response.text}")  
                      
                    result = response.json()  
                  
                # Validate response structure  
                if not self.validate_motif_output(result):  
                    raise ValueError("Invalid motif output structure from miner")  
                      
                return result  
                  
            except (httpx.RequestError, httpx.ConnectError, httpx.ConnectTimeout) as e:  
                if attempt == max_retries - 1:  
                    raise Exception(f"Miner request failed after {max_retries} attempts: {str(e)}")  
                wait_time = 2 ** attempt  # Exponential backoff  
                await asyncio.sleep(wait_time)  
      
    def validate_motif_output(self, output: Dict[str, Any]) -> bool:  
        """Validate miner output structure."""  
        required_keys = ['motifs', 'statistics']  
        if not all(key in output for key in required_keys):  
            return False  
          
        if not isinstance(output['motifs'], list):  
            return False  
              
        return True