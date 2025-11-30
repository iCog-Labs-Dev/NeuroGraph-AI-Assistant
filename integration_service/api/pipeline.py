"""Pipeline API endpoints."""  
import os  
import tempfile  
from typing import List  
from fastapi import APIRouter, UploadFile, File, Form, HTTPException  
from ..services.orchestration_service import OrchestrationService  
from ..config.settings import settings  
  
router = APIRouter()  
orchestration_service = OrchestrationService()  
  
@router.post("/execute")  
async def execute_pipeline(  
    files: List[UploadFile] = File(...),  
    config: str = Form(...),  
    schema_json: str = Form(...),  
    writer_type: str = Form("networkx")  
):  
    """Execute complete pipeline: CSV → NetworkX → Miner."""  
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
          
        # Generate NetworkX graph using AtomSpace Builder  
        networkx_result = await orchestration_service._generate_networkx(  
            csv_files=csv_file_paths,  
            config=config,  
            schema_json=schema_json,  
            writer_type=writer_type,  
            tenant_id="default"  
        )  
          
        # Mine motifs using Neural Miner  
        motifs = await orchestration_service._mine_motifs(  
            networkx_result["networkx_file"]  
        )  
          
        return {  
            "job_id": networkx_result["job_id"],  
            "motifs": motifs,  
            "status": "success"  
        }  
          
    finally:  
        # Cleanup temporary directory  
        import shutil  
        if os.path.exists(temp_dir):  
            shutil.rmtree(temp_dir)