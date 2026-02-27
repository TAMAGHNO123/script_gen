import os
from typing import Dict, Any, Optional

def get_db_connection_params(custom_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Parses a PostgreSQL connection string into a dictionary suitable for psycopg2.connect(**params).
    Supports ?sslmode=require for Neon DB.
    If no custom_url is provided, it falls back to parsing the DATABASE_URL environment variable,
    and then finally falls back to individual PG_HOST, PG_PORT, etc.
    """
    import urllib.parse
    
    url = custom_url or os.getenv("DATABASE_URL")
    
    if url:
        parsed = urllib.parse.urlparse(url)
        params = {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "dbname": parsed.path.lstrip("/") if parsed.path else "postgres",
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
        }
        
        # Parse query params like ?sslmode=require
        if parsed.query:
            query_params = urllib.parse.parse_qs(parsed.query)
            if 'sslmode' in query_params:
                params['sslmode'] = query_params['sslmode'][0]
                
        return params

    # Fallback to individual env vars
    return {
        "host":     os.getenv("PG_HOST",     "localhost"),
        "port":     int(os.getenv("PG_PORT", "5432")),
        "dbname":   os.getenv("PG_DB",       "postgres"),
        "user":     os.getenv("PG_USER",     "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
    }
