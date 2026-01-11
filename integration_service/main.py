"""FastAPI application for Integration Service."""  
from fastapi import FastAPI  
from fastapi.middleware.cors import CORSMiddleware  
from .api.pipeline import router  
from .api.websocket import router as ws_router
from .config.settings import settings  
  
app = FastAPI(  
    title="NeuroGraph Integration Service",  
    description="Orchestration service for Neural Subgraph Mining pipeline",  
    version="1.0.0"  
)  
  
# CORS middleware  
app.add_middleware(  
    CORSMiddleware,  
    allow_origins=["*"],  
    allow_credentials=True,  
    allow_methods=["*"],  
    allow_headers=["*"],  
)  
  
# Include routers  
app.include_router(router, prefix="/api")  
app.include_router(ws_router, prefix="/api")  
  
@app.get("/health")  
async def health_check():  
    """Health check endpoint."""  
    return {"status": "healthy", "service": "integration-service"}  
  
if __name__ == "__main__":  
    import uvicorn  
    uvicorn.run(app, host="0.0.0.0", port=9000)