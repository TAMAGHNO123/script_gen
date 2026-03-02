import json
import yaml
from backend.job_manager import start_job, run_job_sync
from backend.generator import DataGenerator
import time
import os
import shutil

# Make sure we import correctly from backend by fixing path
import sys
sys.path.insert(0, os.path.abspath('d:/script_generator/backend'))

def test_date_range():
    print("Testing Date Range Generation")
    with open("C:/Users/tamag/.gemini/antigravity/brain/0b1b9e71-5309-4e12-8d65-12dfac1285db/test_schema.yml", "r") as f:
        schema = yaml.safe_load(f.read())
        
    print("Initializing Generator with Base Date Range...")
    
    # 1. Test standard init with Start Date and End Date from explicit args
    gen = DataGenerator(
        schema=schema,
        output_dir="test_out_dates",
        target_date_start="2025-01-01",
        target_date_end="2025-01-02",
        rows_per_day=10
    )
    
    assert gen.target_date_start == "2025-01-01"
    assert gen.target_date_end == "2025-01-02"
    
    print("Running generated dump...")
    res = gen.run()
    
    print("Summary Records:", res["total_records"])
    # 2 days * 10 entities * 10 rows per entity = 200 records + existing etc? 
    # Actually wait, gen.run() directly calls daily generation so it should run 2 days * 10 entities * 10 rows = 200 records.
    # plus 0 base generation, as we passed target_date_start directly to generator. wait, standard run without incremental 
    # executes _run_date_range_generation and then also generate files & API dumps?
    print("OK")

if __name__ == "__main__":
    test_date_range()
