# In sagar-backend-api/main.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from config import supabase
import httpx # A modern, async-ready HTTP client

app = FastAPI(
    title="SAGAR Backend Service",
    description="API for querying data and orchestrating analytical jobs.",
    version="2.0.0"
)

# --- Environment variable for the ML service URL ---
# You'll need to add this to your Render environment variables
ML_SERVICE_URL = "https://sagar-ml-service.onrender.com"

# --- Pydantic Models ---
class OrchestrationRequest(BaseModel):
    file1_id: int = Field(..., example=14)
    file2_id: int = Field(..., example=15) # Assuming another file is uploaded
    column1: str = Field(..., example="individualCount")
    column2: str = Field(..., example="sea_surface_temp")

# --- Main Endpoints ---
@app.post("/discover-and-correlate")
async def discover_and_correlate_data(request: OrchestrationRequest):
    """
    Finds file paths from the database and triggers the ML service for analysis.
    """
    try:
        # 1. Query the database to get the file paths from their IDs
        print(f"Fetching metadata for file IDs {request.file1_id} and {request.file2_id}")
        query = supabase.table('file_metadata').select("id, processed_file_location")
        query = query.in_('id', [request.file1_id, request.file2_id])
        db_response = query.execute()

        if not db_response.data or len(db_response.data) < 2:
            raise HTTPException(status_code=404, detail="One or both file IDs not found in the database.")

        # Create a mapping of ID to file path for easy lookup
        file_map = {item['id']: item['processed_file_location'] for item in db_response.data}
        
        # 2. Prepare the request payload for the ML service
        ml_request_payload = {
            "file1_path": file_map[request.file1_id],
            "file2_path": file_map[request.file2_id],
            "column1": request.column1,
            "column2": request.column2
        }

        # 3. Call the ML service to start the background job
        print("Calling ML service to start correlation job...")
        async with httpx.AsyncClient() as client:
            ml_service_endpoint = f"{ML_SERVICE_URL}/analyze/geospatial-correlation"
            response = await client.post(ml_service_endpoint, json=ml_request_payload, timeout=30.0)
            
            # Raise an error if the ML service failed to accept the job
            response.raise_for_status()
            
            ml_job_data = response.json()

        return {
            "message": "Successfully dispatched job to ML service.",
            "ml_service_response": ml_job_data
        }

    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail=f"Error calling ML service: {exc}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/search")
async def search_metadata(file_type: str | None = None):
    """Searches the file_metadata table."""
    try:
        query = supabase.table('file_metadata').select("*")
        if file_type:
            query = query.eq('file_type', file_type)
        response = query.execute()
        return response.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/")
def read_root():
    return {"status": "ok", "message": "SAGAR Backend Service is running."}