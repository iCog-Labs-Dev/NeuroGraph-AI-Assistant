"""Pipeline API endpoints."""  
import os  
import tempfile  
from typing import List, Optional  
from fastapi import APIRouter, UploadFile, File, Form, HTTPException  
from fastapi.responses import FileResponse
from ..services.orchestration_service import OrchestrationService  
from ..config.settings import settings  
  
router = APIRouter()  
orchestration_service = OrchestrationService()


def _parse_int_form(value: Optional[str], default: int) -> int:
    """Parse form value to int; use default if missing, empty, or invalid."""
    if value is None:
        return default
    s = (value.strip() if isinstance(value, str) else str(value)).strip()
    if not s:
        return default
    try:
        return int(s)
    except ValueError:
        return default  
  
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
        # So the UI shows a real error instead of 200 with status error
        if isinstance(result, dict) and result.get("status") == "error":
            raise HTTPException(
                status_code=502,
                detail=result.get("error", "Graph generation failed (AtomSpace error)")
            )
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
    min_pattern_size: Optional[str] = Form("3"),
    max_pattern_size: Optional[str] = Form("5"),
    min_neighborhood_size: Optional[str] = Form("3"),
    max_neighborhood_size: Optional[str] = Form("5"),
    n_neighborhoods: Optional[str] = Form("500"),
    n_trials: Optional[str] = Form("100"),
    out_batch_size: Optional[str] = Form("3"),
    graph_type: Optional[str] = Form(None),
    search_strategy: Optional[str] = Form("greedy"),
    sample_method: Optional[str] = Form("tree"),
    graph_output_format: Optional[str] = Form("representative")
):
    """Mine patterns from NetworkX graph with custom configuration.
    Form values are read as strings and coerced so user-configured values are never dropped.
    """
    # Coerce form strings to ints (client always sends strings via FormData)
    min_ps = _parse_int_form(min_pattern_size, 3)
    max_ps = _parse_int_form(max_pattern_size, 5)
    min_ns = _parse_int_form(min_neighborhood_size, 3)
    max_ns = _parse_int_form(max_neighborhood_size, 5)
    n_neigh = _parse_int_form(n_neighborhoods, 500)
    n_tr = _parse_int_form(n_trials, 100)
    out_bs = _parse_int_form(out_batch_size, 3)

    graph_output_format_val = (graph_output_format or "representative").strip().lower()
    search_strategy_val = (search_strategy or "greedy").strip().lower()
    sample_method_val = (sample_method or "tree").strip().lower()

    # Debug: log raw form values and parsed values so we can trace config flow
    print(
        f"DEBUG /mine-patterns raw form: min_pattern_size={min_pattern_size!r} max_pattern_size={max_pattern_size!r} "
        f"out_batch_size={out_batch_size!r} graph_output_format={graph_output_format!r}",
        flush=True
    )
    print(
        f"DEBUG /mine-patterns parsed: min_pattern_size={min_ps} max_pattern_size={max_ps} "
        f"out_batch_size={out_bs} graph_output_format={graph_output_format_val!r} "
        f"n_trials={n_tr} n_neighborhoods={n_neigh}",
        flush=True
    )

    # Auto-detect graph_type from metadata if not provided
    if not (graph_type and graph_type.strip()):
        graph_type = await orchestration_service.get_graph_type_from_metadata(job_id)
    else:
        graph_type = graph_type.strip().lower()

    mining_config = {
        'min_pattern_size': min_ps,
        'max_pattern_size': max_ps,
        'min_neighborhood_size': min_ns,
        'max_neighborhood_size': max_ns,
        'n_neighborhoods': n_neigh,
        'n_trials': n_tr,
        'out_batch_size': out_bs,
        'graph_type': graph_type,
        'search_strategy': search_strategy_val,
        'sample_method': sample_method_val,
        'graph_output_format': graph_output_format_val
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