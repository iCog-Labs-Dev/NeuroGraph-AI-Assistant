"""Main orchestration service for pipeline coordination."""  
import os  
import uuid  
import httpx  
import tempfile  
import shutil
import json
import asyncio
from typing import Dict, Any, List, Optional  
from .miner_service import MinerService  
from ..config.settings import settings  
  
class OrchestrationService:  
    """Main pipeline orchestrator."""  
            
    def __init__(self):  
        self.miner_service = MinerService()  
        self.atomspace_url = settings.atomspace_url  
        self.timeout = settings.atomspace_timeout  
        self.local_output_dir = "/app/output"
    
    async def generate_networkx(
        self,
        csv_files: List[str],
        config: str,
        schema_json: str,
        writer_type: str,
        graph_type: str = "directed",
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        """Generate NetworkX graph from CSV files, with auxiliary Mork generation in background."""
        import asyncio
        
        mork_task = asyncio.create_task(
            self._generate_auxiliary_mork(
                csv_files,
                config,
                schema_json,
                graph_type,
                tenant_id
            )
        )
        
        try:
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:  
                config_file.write(config)  
                config_path = config_file.name  
                
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as schema_file:  
                schema_file.write(schema_json)  
                schema_path = schema_file.name  
                
            try:  
                async with httpx.AsyncClient(timeout=self.timeout) as client:  
                    files = []  
                    for csv_file_path in csv_files:  
                        csv_file = open(csv_file_path, 'rb')  
                        files.append(('files', (os.path.basename(csv_file_path), csv_file, 'text/csv')))  
                    
                    data = {  
                        'config': config,  
                        'schema_json': schema_json,  
                        'writer_type': writer_type,  
                        'graph_type': graph_type,
                        'tenant_id': tenant_id  
                    }  
                        
                    response = await client.post(  
                        f"{self.atomspace_url}/api/load",  
                        files=files,  
                        data=data  
                    )  
                        
                    for _, (_, file_obj, _) in files:  
                        file_obj.close()  
                        
                    if response.status_code != 200:  
                        raise RuntimeError(f"AtomSpace returned {response.status_code}: {response.text}")  
                        
                    result = response.json()  
                    nx_job_id = result['job_id']
                    asyncio.create_task(self._merge_mork_results(nx_job_id, mork_task))
                    
                networkx_file = f"/shared/output/{nx_job_id}/networkx_graph.pkl"  
                    
                return {
                    "job_id": nx_job_id,
                    "status": "success",
                    "networkx_file": networkx_file
                }
                    
            finally:  
                os.unlink(config_path)  
                os.unlink(schema_path)
                
        except Exception as e:
            return {"status": "error", "error": str(e)}

    async def _generate_auxiliary_mork(
        self,
        csv_files: List[str],
        config: str,
        schema_json: str,
        graph_type: str,
        tenant_id: str
    ) -> Optional[str]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                files = []
                for csv_file_path in csv_files:
                    csv_file = open(csv_file_path, 'rb')
                    files.append(('files', (os.path.basename(csv_file_path), csv_file, 'text/csv')))
                
                data = {
                    'config': config,
                    'schema_json': schema_json,
                    'writer_type': 'mork', 
                    'graph_type': graph_type,
                    'tenant_id': tenant_id
                }
                
                response = await client.post(
                    f"{self.atomspace_url}/api/load",
                    files=files,
                    data=data
                )
                
                for _, (_, file_obj, _) in files:
                    file_obj.close()
                    
                if response.status_code == 200:
                    result = response.json()
                    mork_job_id = result.get('job_id')
                    print(f"Background Mork generation successful. ID: {mork_job_id}")
                    return mork_job_id
                else:
                    print(f"Background Mork generation failed: {response.text}")
                    return None
                    
        except Exception as e:
            print(f"Background Mork generation error: {str(e)}")
            return None

    async def _merge_mork_results(self, nx_job_id: str, mork_task: asyncio.Task):
        """Wait for Mork generation and merge results into NetworkX job folder."""
        try:
            mork_job_id = await mork_task
            
            if not mork_job_id:
                print(f"Skipping merge for {nx_job_id} because Mork generation failed or returned no ID.")
                return

            mork_dir = f"/shared/output/{mork_job_id}"
            nx_dir = f"/shared/output/{nx_job_id}"
            
            if not os.path.exists(mork_dir):
                print(f"Mork output directory not found: {mork_dir}")
                return
                
            if not os.path.exists(nx_dir):
                print(f"NetworkX output directory not found: {nx_dir}")
                return

            mork_subdir = os.path.join(nx_dir, "mork")
            os.makedirs(mork_subdir, exist_ok=True)

            for filename in os.listdir(mork_dir):
                src_path = os.path.join(mork_dir, filename)
                
                if filename in ["schema.json", "neo4j_load_result.json"]:
                    continue
                
                dst_path = os.path.join(mork_subdir, filename)
                
                if os.path.isfile(src_path):
                    shutil.copy2(src_path, dst_path)
            
            shutil.rmtree(mork_dir)
            print(f"Successfully merged Mork files from {mork_job_id} to {mork_subdir}")
            
        except Exception as e:
            print(f"Error merging Mork results: {str(e)}")

    async def mine_patterns(
        self,
        job_id: str,
        mining_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            # Verify NetworkX file exists
            networkx_file = f"/shared/output/{job_id}/networkx_graph.pkl"
            if not os.path.exists(networkx_file):
                raise FileNotFoundError(f"NetworkX file not found for job_id: {job_id}")
            
            graph_output_format = mining_config.get('graph_output_format', 'representative')
            visualize_instances = (graph_output_format == 'instance')
            
            miner_config = mining_config.copy()
            miner_config['visualize_instances'] = visualize_instances
            
            await self.miner_service.mine_motifs(
                networkx_file,
                job_id=job_id,
                mining_config=miner_config
            )
            
            local_paths = self._copy_to_local_output(job_id)
            
            download_url = f"http://localhost:9000/api/download-result?job_id={job_id}"
            
            return {
                "job_id": job_id,
                "status": "success",
                "output_paths": local_paths,
                "download_url": download_url
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    async def get_graph_type_from_metadata(self, job_id: str) -> str:
        """Read graph_type from networkx_metadata.json"""
        metadata_path = f"/shared/output/{job_id}/networkx_metadata.json"
        
        if not os.path.exists(metadata_path):
            metadata_path = f"/shared/output/{job_id}/job_metadata.json"
            
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(
                f"Metadata file not found for job_id: {job_id} "
                f"(checked networkx_metadata.json and job_metadata.json)"
            )
        
        try:
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            graph_type = metadata.get('graph_type', 'directed')
            print(f"Auto-detected graph_type='{graph_type}' from metadata for job_id={job_id}")
            return graph_type
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid metadata file for job_id: {job_id}: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Error reading metadata for job_id: {job_id}: {str(e)}")
    
    def _copy_to_local_output(self, job_id: str) -> Dict[str, str]:
        """Copy results from shared volume to local directory and return paths."""
        shared_job_dir = f"/shared/output/{job_id}"
        local_job_dir = f"{self.local_output_dir}/{job_id}"
        
        os.makedirs(local_job_dir, exist_ok=True)
        
        shared_results = f"{shared_job_dir}/results"
        local_results = f"{local_job_dir}/results"
        if os.path.exists(shared_results):
            if os.path.exists(local_results):
                shutil.rmtree(local_results)
            shutil.copytree(shared_results, local_results)
        
        shared_plots = f"{shared_job_dir}/plots"
        local_plots = f"{local_job_dir}/plots"
        if os.path.exists(shared_plots):
            if os.path.exists(local_plots):
                shutil.rmtree(local_plots)
            shutil.copytree(shared_plots, local_plots)
        
        return {
            "results": f"./integration_service/output/{job_id}/results",
            "plots": f"./integration_service/output/{job_id}/plots"
        }

    def get_result_file_path(self, job_id: str, filename: str) -> str:
    
        job_dir = os.path.abspath(os.path.join(self.local_output_dir, job_id))
        file_path = os.path.abspath(os.path.join(job_dir, filename))
        
        if not file_path.startswith(job_dir):
            raise PermissionError("Access denied: Invalid file path")
            
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {filename}")
            
        return file_path

    def create_job_archive(self, job_id: str) -> str:
        """
        Create a zip archive of the entire job directory.
        """
        job_dir = os.path.join(self.local_output_dir, job_id)
        if not os.path.exists(job_dir):
            raise FileNotFoundError(f"Job directory not found: {job_id}")
        
        zip_base_name = os.path.join(self.local_output_dir, f"{job_id}")
        zip_file_path = f"{zip_base_name}.zip"
        
        shutil.make_archive(zip_base_name, 'zip', job_dir)
        
        return zip_file_path