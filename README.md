# Data Forge Platform

Data Forge Platform is a full-stack, schema-driven, large-scale realistic mock data generator. It parses a YAML schema and programmatically generates millions of rows of mock data across PostgreSQL, CSV, Excel, Parquet, and JSON API dumps while handling complex relationships, chunking, and simulated messiness.

## Features
- **Frontend Dashboard:** A React + Vite web dashboard featuring a YAML schema editor, beautiful animations, and live status parsing of data generation jobs.
- **Asynchronous Processing:** Python FastAPI backend leverages an in-memory job queue to process massive schema generations (2-4 million rows) asynchronously in the background.
- **Topological Sorting:** Automatically detects and sorts Foreign Key dependency chains across the schema entities.
- **Postgres Bulk-Copy:** Optimized PostgreSQL driver loading via `StringIO` to natively pipe CSV data straight into database tables for high-performance writes.
- **Realistic Messiness Injection:** Features built-in mechanics for null injection, duplicate keys, orphaned records, stale watermarks, and schema-varying field inclusion.
- **Pagination & Exporters:** Native output to Parquet using PyArrow, Excel using openpyxl, and paginated JSON dumps simulating actual APIs.

## Architecture Structure

```text
d:\script_generator\
├── frontend/                 # React, TypeScript, Vite, Tailwind CSS, Framer Motion
│   ├── src/
│   │   ├── main.tsx          # React Entrypoint
│   │   ├── App.tsx           # Dashboard UI and REST Logic
│   │   └── index.css         # Tailwind directives
│   ├── package.json
│   ├── vite.config.ts
│   └── tailwind.config.js
└── backend/                  # Python, FastAPI, Pandas, PyArrow
    ├── main.py               # FastAPI Server and Routing (Entrypoint)
    ├── generator.py          # Core Object-Oriented Faker/Pandas Generator
    ├── job_manager.py        # Asynchronous ThreadPool Job Queue
    └── requirements.txt
```

---

## 🚀 Getting Started

Ensure you have **Python 3.12+** and **Node.js 18+** installed. You will also need PostgreSQL running locally if you intend to test the DB sink.

### 1. Backend Setup (Virtual Environment)
The backend requires several data engineering libraries. It is highly recommended to run this within a virtual environment.

```powershell
cd backend

# Create Virtual Environment
python -m venv .venv

# Activate Virtual Environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Install Dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables
By default, the backend will listen to standard environment variables to connect to PostgreSQL. However, if you are testing the output locally or on a cloud provider like Neon DB, the **easiest way** to configure this is through the UI.

In the `backend/.env.example` and `frontend/.env.example` you can see standard settings.
*Note: For Neon DB, you must ensure your connection string includes `?sslmode=require`.*

### 3. Run the Backend API
```powershell
# Inside the backend folder with the venv activated:
uvicorn main:app --port 8000
```
This will start the FastAPI backend answering requests on `http://localhost:8000`.

### 4. Frontend Setup
```powershell
cd frontend

# Install Node modules
npm install

# Start the Vite Dev Server
npm run dev
```
Open your browser to `http://localhost:5173/`. 

---

## ⚡ Usage Instructions

1. Navigate to the web dashboard (`http://localhost:5173/`).
2. (Optional) Click **"DB Settings"** in the top right. Here, you can insert a temporary runtime connection string override. You can hit **"Test Connection"** to verify your local instance or Neon DB string works before proceeding.
3. You will see the **YAML Schema** editor prepopulated with a sample schema. You can edit this in place.
4. Once satisfied, click **"Mock Me"**.
5. The frontend will dispatch the schema and your connection override to the `/generate` endpoint.
6. Watch the dashboard flip into a pulsing loading state as it checks the backend's `/status/{job_id}` endpoint every 2 seconds.
7. Alternatively, inspect the backend terminal to see live logs of the chunks generating and database ingestion taking place in real-time.
8. Upon completion, the front end will slide up the complete, parsed outcome (Rows generated, Execution Times, DB/File summaries).

### Generated Files
All artifacts (CSV, Parquet, JSON, Excel) will be placed inside dynamically generated subdirectories underneath `backend/output_{job_id}/`.

---

## 📌 API Contract

#### `POST /generate`
Accepts a raw YAML body payload. Returns a job track ID.
```json
{
  "job_id": "8e3...4f",
  "status": "pending"
}
```

#### `GET /status/{job_id}`
Returns the current asynchronous job state.
```json
{
  "status": "running" // (can be 'pending', 'running', 'completed', 'failed', 'not_found')
}
```

#### `GET /result/{job_id}`
Returns the finalized JSON output.
```json
{
  "status": "completed",
  "result": {
    "execution_seconds": 12.5,
    "total_records": 4000000,
    "database_tables": {},
    "files_generated": [],
    "api_dumps_generated": []
  }
}
```

#### `GET /api-data/{job_id}`
Lists all available API dumps for a completed job.
```json
{
  "job_id": "8e3...4f",
  "api_dumps": [
    {
      "name": "courier_status_api",
      "total_records": 5000,
      "pages": 50,
      "browse_url": "/api-data/8e3...4f/courier_status_api?page=1"
    }
  ]
}
```

#### `GET /api-data/{job_id}/{api_name}?page=1`
Returns the raw paginated JSON data for a specific API dump. Hit this endpoint in your browser to see the generated data.
```json
{
  "api": "courier_status_api",
  "page": 1,
  "page_size": 100,
  "total_pages": 50,
  "total_records": 5000,
  "data": [ { "tracking_number": "1Z054NZD...", "status": "IN_TRANSIT" } ]
}
```
