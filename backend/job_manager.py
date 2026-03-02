import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor
from generator import DataGenerator

jobs = {}
executor = ThreadPoolExecutor(max_workers=2)

from typing import Optional

def run_job_sync(job_id: str, schema: dict, connection_string: Optional[str] = None,
                 incremental: bool = False, incremental_rows: int = 0,
                 base_job_id: Optional[str] = None,
                 target_date_start: Optional[str] = None,
                 target_date_end: Optional[str] = None,
                 rows_per_day: int = 10):
    try:
        jobs[job_id]["status"] = "running"
        # In incremental / daily mode, reuse the base job's output directory
        output_dir = f"output_{base_job_id}" if base_job_id else f"output_{job_id}"
        generator = DataGenerator(
            schema=schema,
            output_dir=output_dir,
            connection_string=connection_string,
            incremental=incremental,
            incremental_rows=incremental_rows,
            target_date_start=target_date_start,
            target_date_end=target_date_end,
            rows_per_day=rows_per_day,
        )
        summary = generator.run()
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["result"] = summary
        # Store the effective output dir so the frontend can find files
        jobs[job_id]["output_dir"] = output_dir
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        import traceback
        traceback.print_exc()

def start_job(schema: dict, connection_string: Optional[str] = None,
              incremental: bool = False, incremental_rows: int = 0,
              base_job_id: Optional[str] = None,
              target_date_start: Optional[str] = None,
              target_date_end: Optional[str] = None,
              rows_per_day: int = 10) -> str:
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "result": None, "error": None}
    
    # Run in executor
    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor, run_job_sync, job_id, schema, connection_string,
        incremental, incremental_rows, base_job_id,
        target_date_start, target_date_end, rows_per_day,
    )
    
    return job_id

def get_job_status(job_id: str) -> dict:
    if job_id not in jobs:
        return {"status": "not_found"}
    return {"status": jobs[job_id]["status"]}

def get_job_result(job_id: str) -> dict:
    if job_id not in jobs:
        return {"error": "Job not found"}
    return jobs[job_id]
