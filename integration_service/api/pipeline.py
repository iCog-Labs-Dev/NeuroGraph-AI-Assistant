"""Pipeline API endpoints."""  
import os  
import tempfile  
from fastapi import APIRouter, UploadFile, File, Form, HTTPException  
from ..services.orchestration_service import OrchestrationService  
from ..config.settings import settings  
  
router = APIRouter()  
orchestration_service = OrchestrationService()  
  
@router.post("/execute")  
async def execute_pipeline(  
    file: UploadFile = File(...),  
    config: str = Form(...),  
    schema_json: str = Form(...),  
    tenant_id: str = Form("default"),  
    session_id: str = Form(None)  
):  
    """Execute complete pipeline: CSV → NetworkX → Miner."""  
    if not file.filename.endswith('.csv'):  
        raise HTTPException(status_code=400, detail="Only CSV files are supported")  
      
    # Save uploaded file temporarily  
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp_file:  
        content = await file.read()  
        tmp_file.write(content)  
        tmp_file_path = tmp_file.name  
      
    try:  
        # Execute complete mining pipeline with config/schema  
        result = await orchestration_service.execute_mining_pipeline(  
            csv_file_path=tmp_file_path,  
            config=config,  
            schema_json=schema_json,  
            tenant_id=tenant_id,  
            session_id=session_id  
        )  
        return result  
    finally:  
        # Cleanup temporary file  
        os.unlink(tmp_file_path)  
  
@router.post("/select-motif")  
async def select_motif(  
    job_id: str = Form(...),  
    motif_index: int = Form(...),  
    tenant_id: str = Form("default")  
):  
    """User selects a motif for processing."""  
    try:  
        result = await orchestration_service.process_selected_motif(  
            job_id=job_id,  
            motif_index=motif_index,  
            tenant_id=tenant_id  
        )  
        return result  
    except Exception as e:  
        raise HTTPException(status_code=500, detail=str(e))