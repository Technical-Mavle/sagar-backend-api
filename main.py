# In sagar-backend-api/main.py

from fastapi import FastAPI, HTTPException
from config import supabase # Import the client from your config file

app = FastAPI(
    title="SAGAR Backend Service",
    description="API for querying and retrieving marine data.",
    version="1.0.0"
)

@app.get("/search")
async def search_metadata(file_type: str | None = None):
    """
    Searches the file_metadata table.
    Optionally filters by file_type (e.g., 'image', 'tabular').
    """
    try:
        query = supabase.table('file_metadata').select("*")

        if file_type:
            query = query.eq('file_type', file_type)
        
        response = query.execute()

        return response.data
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"An error occurred: {str(e)}"
        )

@app.get("/")
def read_root():
    return {"status": "ok", "message": "SAGAR Backend Service is running."}