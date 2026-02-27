import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor
from generator import DataGenerator

jobs = {}
executor = ThreadPoolExecutor(max_workers=2)

from typing import Optional

def run_job_sync(job_id: str, schema: dict, connection_string: Optional[str] = None):
    try:
        jobs[job_id]["status"] = "running"
        generator = DataGenerator(schema=schema, output_dir=f"output_{job_id}", connection_string=connection_string)
        summary = generator.run()
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["result"] = summary
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        import traceback
        traceback.print_exc()

def start_job(schema: dict, connection_string: Optional[str] = None) -> str:
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "result": None, "error": None}
    
    # Run in executor
    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, run_job_sync, job_id, schema, connection_string)
    
    return job_id

def get_job_status(job_id: str) -> dict:
    if job_id not in jobs:
        return {"status": "not_found"}
    return {"status": jobs[job_id]["status"]}

def get_job_result(job_id: str) -> dict:
    if job_id not in jobs:
        return {"error": "Job not found"}
    return jobs[job_id]
