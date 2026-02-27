import os
import mimetypes
import yaml
import json
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from job_manager import start_job, get_job_status, get_job_result
from schema_adapter import adapt_schema

BACKEND_DIR = Path(__file__).parent.resolve()

app = FastAPI(title="Data Generator Platform API")

# Allow CORS for local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/generate")
async def generate_data(request: Request):
    try:
        body_bytes = await request.body()
        body_dict = json.loads(body_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
        
    schema_str = body_dict.get("schema")
    connection_string = body_dict.get("connection_string")
    
    if not schema_str:
        raise HTTPException(status_code=400, detail="Missing 'schema' in payload")

    try:
        schema_dict = yaml.safe_load(schema_str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {str(e)}")
        
    if not isinstance(schema_dict, dict):
        raise HTTPException(status_code=400, detail="YAML must parse to an object/dict")

    # Normalise: convert user-friendly format to internal generator format
    try:
        schema_dict = adapt_schema(schema_dict)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Schema adaptation error: {str(e)}")

    job_id = start_job(schema_dict, connection_string)
    return {"job_id": job_id, "status": "pending"}

@app.post("/test-connection")
async def test_connection(request: Request):
    try:
        body_bytes = await request.body()
        body_dict = json.loads(body_bytes)
        connection_string = body_dict.get("connection_string")
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    from db_manager import get_db_connection_params
    import psycopg2
    
    params = get_db_connection_params(connection_string)
    
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Connection successful"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/status/{job_id}")
async def status(job_id: str):
    res = get_job_status(job_id)
    if res.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="Job not found")
    return res

@app.get("/result/{job_id}")
async def result(job_id: str):
    res = get_job_result(job_id)
    if "error" in res and res["error"] == "Job not found":
        raise HTTPException(status_code=404, detail="Job not found")
        
    if res.get("status") == "failed":
        return {"status": "failed", "error": res.get("error")}
        
    if res.get("status") != "completed":
        return {"status": res.get("status")}
        
    return {
        "status": "completed",
        "result": res.get("result")
    }

@app.get("/files/{job_id}")
async def list_files(job_id: str):
    """List all generated files for a completed job."""
    res = get_job_result(job_id)
    if "error" in res and res["error"] == "Job not found":
        raise HTTPException(status_code=404, detail="Job not found")
    if res.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job not completed yet")

    result = res.get("result", {})
    files = result.get("files_generated", [])
    # Attach a relative path so frontend can request download
    enriched = []
    for f in files:
        abs_path = Path(f["filename"]).resolve()
        try:
            rel = abs_path.relative_to(BACKEND_DIR)
        except ValueError:
            rel = abs_path
        enriched.append({**f, "rel_path": str(rel).replace("\\", "/")})
    return {"files": enriched}


@app.get("/download")
async def download_file(job_id: str = Query(...), path: str = Query(...)):
    """Stream a generated file back to the browser as an attachment."""
    # Security: resolve and ensure the file sits under backend dir
    target = (BACKEND_DIR / path).resolve()
    try:
        target.relative_to(BACKEND_DIR)
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    # Verify the file belongs to the given job's output directory
    expected_prefix = BACKEND_DIR / f"output_{job_id}"
    try:
        target.relative_to(expected_prefix)
    except ValueError:
        raise HTTPException(status_code=403, detail="File does not belong to this job")

    media_type, _ = mimetypes.guess_type(str(target))
    if not media_type:
        media_type = "application/octet-stream"

    return FileResponse(
        path=str(target),
        media_type=media_type,
        filename=target.name,
        headers={"Content-Disposition": f'attachment; filename="{target.name}"'},
    )


# ---------------------------------------------------------------------------
# API Data Browse Endpoints
# ---------------------------------------------------------------------------

@app.get("/api-data/{job_id}")
async def list_api_dumps(job_id: str):
    """List all available API dumps for a completed job."""
    res = get_job_result(job_id)
    if "error" in res and res["error"] == "Job not found":
        raise HTTPException(status_code=404, detail="Job not found")
    if res.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job not completed yet")

    output_dir = BACKEND_DIR / f"output_{job_id}"
    if not output_dir.exists():
        raise HTTPException(status_code=404, detail="Output directory not found")

    result = res.get("result", {})
    api_dumps = result.get("api_dumps_generated", [])

    apis = []
    for dump in api_dumps:
        api_name = dump["name"]
        api_dir = output_dir / api_name
        page_count = len(list(api_dir.glob("*.json"))) if api_dir.exists() else 0
        apis.append({
            "name": api_name,
            "total_records": dump.get("records", 0),
            "pages": dump.get("pages", page_count),
            "size_kb": dump.get("size_kb", 0),
            "browse_url": f"/api-data/{job_id}/{api_name}?page=1",
        })
    return {"job_id": job_id, "api_dumps": apis}


@app.get("/api-data/{job_id}/{api_name}")
async def browse_api_data(job_id: str, api_name: str, page: int = Query(1, ge=1)):
    """Serve paginated JSON data for a specific API dump — hit this to see the data."""
    res = get_job_result(job_id)
    if "error" in res and res["error"] == "Job not found":
        raise HTTPException(status_code=404, detail="Job not found")
    if res.get("status") != "completed":
        raise HTTPException(status_code=400, detail="Job not completed yet")

    api_dir = (BACKEND_DIR / f"output_{job_id}" / api_name).resolve()
    # Security: ensure path stays under backend dir
    try:
        api_dir.relative_to(BACKEND_DIR)
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    if not api_dir.exists() or not api_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"API dump '{api_name}' not found")

    page_file = api_dir / f"page_{page}.json"
    if not page_file.exists():
        # Find all pages to report max
        all_pages = sorted(api_dir.glob("page_*.json"))
        max_page = len(all_pages)
        raise HTTPException(
            status_code=404,
            detail=f"Page {page} not found. This API dump has {max_page} page(s). Use ?page=1 through ?page={max_page}",
        )

    with open(page_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Data Generator API is running"}
