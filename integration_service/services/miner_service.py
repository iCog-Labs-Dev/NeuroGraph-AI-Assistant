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
      
    async def mine_motifs(
        self, 
        networkx_file_path: str, 
        job_id: str = None,
        mining_config: Dict[str, Any] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:  
        """Send NetworkX file to miner with config and return discovered motifs."""  
        if not os.path.exists(networkx_file_path):  
            raise FileNotFoundError(f"NetworkX file not found: {networkx_file_path}")  
          
        if job_id is None:
            path_parts = networkx_file_path.split('/')
            if len(path_parts) >= 4 and path_parts[-3] == 'output':
                job_id = path_parts[-2]
        
        if mining_config is None:
            mining_config = {}
    
        for attempt in range(max_retries):  
            try:  
                # Read NetworkX file  
                with open(networkx_file_path, 'rb') as f:  
                    networkx_data = f.read()  
                  
                # Build form data from user config only; send as strings so miner form parsing works
                data = {}
                if job_id:
                    data['job_id'] = str(job_id)
                data['min_pattern_size'] = str(mining_config.get('min_pattern_size', 3))
                data['max_pattern_size'] = str(mining_config.get('max_pattern_size', 5))
                data['min_neighborhood_size'] = str(mining_config.get('min_neighborhood_size', 3))
                data['max_neighborhood_size'] = str(mining_config.get('max_neighborhood_size', 7))
                data['n_neighborhoods'] = str(mining_config.get('n_neighborhoods', 500))
                data['n_trials'] = str(mining_config.get('n_trials', 100))
                data['radius'] = str(mining_config.get('radius', 3))
                data['graph_type'] = str(mining_config.get('graph_type', 'directed'))
                data['search_strategy'] = str(mining_config.get('search_strategy', 'greedy'))
                data['sample_method'] = str(mining_config.get('sample_method', 'tree'))
                data['out_batch_size'] = str(mining_config.get('out_batch_size', 3))
                data['visualize_instances'] = 'true' if mining_config.get('visualize_instances') else 'false'

                # Debug: log what we send to the miner
                print(
                    f"DEBUG miner_service sending to miner: min_pattern_size={data.get('min_pattern_size')} "
                    f"max_pattern_size={data.get('max_pattern_size')} out_batch_size={data.get('out_batch_size')} "
                    f"visualize_instances={data.get('visualize_instances')}",
                    flush=True
                )

                # Send to miner using HTTP client  
                async with httpx.AsyncClient(timeout=self.timeout) as client:  
                    files = {'graph_file': ('graph.gpickle', networkx_data, 'application/octet-stream')}
                    
                    response = await client.post(f"{self.miner_url}/mine", files=files, data=data)  
                      
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
        required_keys = ['results_path', 'plots_path', 'status']  
        if not all(key in output for key in required_keys):  
            return False  

        return True