"""Pipeline API endpoints."""  
import os  
import tempfile  
from typing import List  
from fastapi import APIRouter, UploadFile, File, Form, HTTPException  
from fastapi.responses import FileResponse
from ..services.orchestration_service import OrchestrationService  
from ..config.settings import settings  
  
router = APIRouter()  
orchestration_service = OrchestrationService()  
  
@router.post("/generate-graph")  
async def generate_graph(  
    files: List[UploadFile] = File(...),  
    config: str = Form(...),  
    schema_json: str = Form(...),  
    writer_type: str = Form("networkx"),
    graph_type: str = Form("directed")
):  
    """Generate NetworkX graph from CSV files."""  
    print(f"DEBUG: Received generate-graph request with files: {[f.filename for f in files]}")
    # Validate all files are CSV  
    for file in files:  
        if not file.filename.endswith('.csv'):  
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")  
      
    # Save uploaded files to temporary directory  
    temp_dir = tempfile.mkdtemp()  
    csv_file_paths = []  
      
    try:  
        for file in files:  
            file_path = os.path.join(temp_dir, file.filename)  
            with open(file_path, "wb") as f:  
                content = await file.read()  
                f.write(content)  
            csv_file_paths.append(file_path)  
          
        result = await orchestration_service.generate_networkx(
            csv_files=csv_file_paths,
            config=config,
            schema_json=schema_json,
            writer_type=writer_type,
            graph_type=graph_type,
            tenant_id="default",
            cleanup_dir=temp_dir
        )
        print(f"DEBUG: generate_networkx completed. Result: {result}")
          
        return result
          
    except Exception as e:
        print(f"DEBUG: Error in generate-graph endpoint: {e}")
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise e

@router.post("/mine-patterns")
async def mine_patterns(
    job_id: str = Form(...),
    min_pattern_size: int = Form(3),
    max_pattern_size: int = Form(5),
    min_neighborhood_size: int = Form(3),
    max_neighborhood_size: int = Form(5),
    n_neighborhoods: int = Form(500),
    n_trials: int = Form(100),
    graph_type: str = Form(None),
    search_strategy: str = Form("greedy"),
    sample_method: str = Form("tree"),
    graph_output_format: str = Form("representative"),
    out_batch_size: int = Form(3)
):
    """ Mine patterns from NetworkX graph with custom configuration."""
    
    # Auto-detect graph_type from metadata if not provided
    if graph_type is None:
        graph_type = await orchestration_service.get_graph_type_from_metadata(job_id)
    
    mining_config = {
        'min_pattern_size': min_pattern_size,
        'max_pattern_size': max_pattern_size,
        'min_neighborhood_size': min_neighborhood_size,
        'max_neighborhood_size': max_neighborhood_size,
        'n_neighborhoods': n_neighborhoods,
        'n_trials': n_trials,
        'graph_type': graph_type,
        'search_strategy': search_strategy,
        'sample_method': sample_method,
        'graph_output_format': graph_output_format,
        'out_batch_size': out_batch_size
    }
    
    result = await orchestration_service.mine_patterns(
        job_id=job_id,
        mining_config=mining_config
    )
    
    return result

@router.get("/download-result")
async def download_result(job_id: str, filename: str = None):
    try:
        if filename:
            # Download specific file
            file_path = orchestration_service.get_result_file_path(job_id, filename)
            return FileResponse(
                path=file_path,
                filename=os.path.basename(file_path),
                media_type='application/octet-stream'
            )
        else:
            # Download entire job as ZIP
            zip_path = orchestration_service.create_job_archive(job_id)
            return FileResponse(
                path=zip_path,
                filename=f"{job_id}.zip",
                media_type='application/zip'
            )
            
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/mining-status/{job_id}")
async def get_mining_status(job_id: str):
    """Get the current progress of a mining job."""
    try:
        # Define path to progress file in shared volume
        # Note: We access it via the shared volume path
        progress_path = f"/shared/output/{job_id}/progress.json"
        
        if not os.path.exists(progress_path):
            # If no progress file yet, return pending status
            return {
                "status": "pending", 
                "progress": 0, 
                "message": "Waiting for miner to start..."
            }
            
        import json
        with open(progress_path, 'r') as f:
            status_data = json.load(f)
            
        return status_data
        
    except Exception as e:
        # Don't fail the request, just return error status
        return {
            "status": "error", 
            "progress": 0, 
            "message": f"Error checking status: {str(e)}"
        }