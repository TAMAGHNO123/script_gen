import urllib.request
import json
import time

schema_text = """
project: test_generation
version: "1.0.0"
temporal:
  start_date: "2026-03-02"
  end_date: "2026-03-04"
global_settings:
  null_pct: 0.05

entities:
  - name: db_table_01
    source_type: database
    volume: 10
    columns:
      - { name: id, type: uuid, primary_key: true }
      - { name: date, type: date, pattern: temporal_growth }
"""

req = urllib.request.Request(
    "http://localhost:8000/generate",
    data=json.dumps({"schema": schema_text}).encode("utf-8"),
    headers={"Content-Type": "application/json"}
)
with urllib.request.urlopen(req) as response:
    job = json.loads(response.read().decode())
job_id = job["job_id"]
print("Started job:", job_id)

while True:
    with urllib.request.urlopen(f"http://localhost:8000/status/{job_id}") as response:
        st = json.loads(response.read().decode())
    status = st.get("status")
    print("Status:", status)
    if status in ["completed", "failed"]:
        with urllib.request.urlopen(f"http://localhost:8000/result/{job_id}") as response:
            out = json.loads(response.read().decode())
            print(json.dumps(out, indent=2))
        break
    time.sleep(1)
