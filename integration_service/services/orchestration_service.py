"""Main orchestration service for pipeline coordination."""  
import os  
import uuid  
import httpx  
import tempfile  
import json  
import shutil  
from typing import Dict, Any, List  
from .miner_service import MinerService  
from ..config.settings import settings  
  
class OrchestrationService:  
    """Main pipeline orchestrator."""  
          
    def __init__(self):  
        self.miner_service = MinerService()  
        self.atomspace_url = settings.atomspace_url  
        self.timeout = settings.atomspace_timeout  
          
    async def _generate_networkx(  
        self,  
        csv_files: List[str],  
        config: str,  
        schema_json: str,  
        writer_type: str,  
        tenant_id: str = "default"  
    ) -> Dict[str, Any]:  
        """Generate NetworkX graph using AtomSpace Builder API."""  
          
        try:  
            # Create temporary files for config and schema  
            config_path = os.path.join(tempfile.gettempdir(), "config.json")  
            schema_path = os.path.join(tempfile.gettempdir(), "schema.json")  
              
            with open(config_path, 'w') as f:  
                f.write(config)  
            with open(schema_path, 'w') as f:  
                f.write(schema_json)  
              
            # Prepare files for upload  
            files = []  
            for csv_file in csv_files:  
                files.append(('files', open(csv_file, 'rb')))  
              
            data = {  
                'config': config,  
                'schema_json': schema_json,  
                'writer_type': writer_type  
            }  
              
            async with httpx.AsyncClient(timeout=self.timeout) as client:  
                response = await client.post(  
                    f"{self.atomspace_url}/api/load",  
                    files=files,  
                    data=data  
                )  
              
            if response.status_code != 200:  
                raise Exception(f"AtomSpace API error: {response.text}")  
              
            result = response.json()  
              
            # Close all opened files  
            for _, file_obj in files:  
                file_obj.close()  
              
            # Copy generated files to shared volume  
            await self._copy_to_shared_volume(result['job_id'], result['output_dir'])  
              
            # Return with correct NetworkX file path  
            networkx_file = f"/shared/output/{result['job_id']}/networkx_graph.pkl"  
              
            return {  
                "job_id": result['job_id'],  
                "networkx_file": networkx_file  
            }  
              
        finally:  
            # Cleanup temporary files  
            if os.path.exists(config_path):  
                os.unlink(config_path)  
            if os.path.exists(schema_path):  
                os.unlink(schema_path)  
      
    async def _copy_to_shared_volume(self, job_id: str, local_output_dir: str):  
        """Copy generated files from local output to shared volume."""  
        try:  
            shared_dir = f"/shared/output/{job_id}"  
            os.makedirs(shared_dir, exist_ok=True)  
              
            # Copy all files from local output to shared volume  
            for filename in os.listdir(local_output_dir):  
                src_path = os.path.join(local_output_dir, filename)  
                dst_path = os.path.join(shared_dir, filename)  
                  
                if os.path.isfile(src_path):  
                    shutil.copy2(src_path, dst_path)  
                    print(f"Copied {filename} to shared volume")  
                      
        except Exception as e:  
            print(f"Warning: Failed to copy files to shared volume: {e}")  
      
    async def _mine_motifs(self, networkx_file_path: str) -> Dict[str, Any]:  
        """Mine motifs using Neural Miner service."""  
        return await self.miner_service.mine_motifs(networkx_file_path)