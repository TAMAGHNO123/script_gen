import json
import math
import os
import random
import string
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from faker import Faker  # type: ignore

try:
    import psycopg2  # type: ignore
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    import openpyxl  # type: ignore
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

# ---------------------------------------------------------------------------
# Faker pool helpers
# ---------------------------------------------------------------------------
POOL_SIZE = 5000

def _make_pool(fake: Faker, col_type: str, col_def: Dict, n_pool: int = POOL_SIZE) -> np.ndarray:
    ct = col_type.lower()
    if ct == "uuid":
        return np.array([str(fake.uuid4()) for _ in range(n_pool)], dtype=object)
    elif ct == "string":
        pattern = col_def.get("pattern", "????????")
        return np.array([_fill_pattern(pattern) for _ in range(n_pool)], dtype=object)
    elif ct in ("name", "person"):
        return np.array([fake.name() for _ in range(n_pool)], dtype=object)
    elif ct == "company":
        return np.array([fake.company() for _ in range(n_pool)], dtype=object)
    elif ct == "email":
        return np.array([fake.email() for _ in range(n_pool)], dtype=object)
    elif ct == "phone":
        return np.array([fake.phone_number() for _ in range(n_pool)], dtype=object)
    elif ct == "address":
        return np.array([fake.address().replace("\n", ", ") for _ in range(n_pool)], dtype=object)
    elif ct == "job_title":
        return np.array([fake.job() for _ in range(n_pool)], dtype=object)
    elif ct == "state":
        return np.array([fake.state_abbr() for _ in range(n_pool)], dtype=object)
    elif ct == "ipv4":
        return np.array([fake.ipv4() for _ in range(n_pool)], dtype=object)
    elif ct == "country":
        return np.array([fake.country() for _ in range(n_pool)], dtype=object)
    elif ct == "city":
        return np.array([fake.city() for _ in range(n_pool)], dtype=object)
    elif ct == "text":
        return np.array([fake.sentence(nb_words=10) for _ in range(n_pool)], dtype=object)
    elif ct == "product_name":
        adjectives = ["Premium","Standard","Bulk","Ultra","Eco","Pro","Industrial","Advanced","Basic","Elite"]
        nouns      = ["Widget","Gear","Module","Unit","Pack","Set","Component","Assembly","Device","System"]
        return np.array(
            [f"{random.choice(adjectives)} {random.choice(nouns)} {fake.bothify('??-###')}"
             for _ in range(n_pool)], dtype=object)
    elif ct == "boolean":
        return np.random.choice([True, False], size=n_pool)
    elif ct == "choice":
        choices = col_def.get("choices", ["unknown"])
        return np.random.choice(choices, size=n_pool)
    else:
        return np.array([None] * n_pool, dtype=object)

def _fill_pattern(pattern: str) -> str:
    result: List[str] = []
    for ch in pattern:
        if ch == "#":
            result.append(random.choice(string.digits))
        elif ch == "?":
            result.append(random.choice(string.ascii_uppercase))
        elif ch == "*":
            result.append(random.choice(string.ascii_uppercase + string.digits))
        else:
            result.append(str(ch))
    return "".join(result)


# ---------------------------------------------------------------------------
# DataGenerator Class
# ---------------------------------------------------------------------------
class DataGenerator:
    def __init__(self, schema: Dict, output_dir: str = "output", connection_string: Optional[str] = None,
                 incremental: bool = False, incremental_rows: int = 0):
        import importlib, sys, os as _os
        _backend_dir = _os.path.dirname(_os.path.abspath(__file__))
        if _backend_dir not in sys.path:
            sys.path.insert(0, _backend_dir)
        db_manager = importlib.import_module("db_manager")
        self.pg_conn_params = db_manager.get_db_connection_params(connection_string)
        self.schema = schema
        self.output_dir = Path(output_dir)
        self.incremental = incremental
        self.incremental_rows = incremental_rows
        self.start_time = time.time()
        self.summary: Dict[str, Any] = {
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "schema_version": schema.get("version", "unknown"),
            "incremental": incremental,
            "incremental_rows": incremental_rows if incremental else 0,
            "database_tables": {},
            "files_generated": [],
            "api_dumps_generated": [],
            "total_records": 0,
            "execution_seconds": 0,
        }
        self.fake = Faker()
        Faker.seed(42)
        np.random.seed(42)
        random.seed(42)
        
        # Let's make api_dumps and file_sources optional by providing default empty lists if missing.
        if "api_dumps" not in self.schema:
            self.schema["api_dumps"] = []
        if "file_sources" not in self.schema:
            self.schema["file_sources"] = []
            
        required_keys = {"temporal", "global_messiness", "database", "file_sources", "api_dumps"}
        missing = required_keys - schema.keys()
        if missing:
            raise ValueError(f"Schema is missing top-level keys: {missing}")
            
    def _temporal_config(self) -> Tuple[datetime, datetime, Dict]:
        t = self.schema["temporal"]
        start = datetime.strptime(t["start_date"], "%Y-%m-%d")
        end   = datetime.strptime(t["end_date"],   "%Y-%m-%d")
        return start, end, t

    def _random_timestamps(self, n: int, start: datetime, end: datetime,
                            temporal_cfg: Dict,
                            intro_offset_days: int = 0,
                            pattern: str = "") -> np.ndarray:
        range_start = start + timedelta(days=intro_offset_days)
        total_secs  = max(int((end - range_start).total_seconds()), 1)

        if pattern == "temporal_growth":
            ratios = np.random.beta(5, 1, size=n)
            raw = (ratios * total_secs).astype(np.float64)
        else:
            raw = np.random.randint(0, total_secs, size=n).astype(np.float64)

        base_ts = np.array([range_start.timestamp()], dtype=np.float64) + raw

        late_pct  = temporal_cfg.get("late_arriving_pct", 0.0)
        late_max  = temporal_cfg.get("late_arriving_max_days", 10) * 86400
        if late_pct > 0:
            late_mask = np.random.random(n) < late_pct
            base_ts[late_mask] -= np.random.randint(0, late_max, size=late_mask.sum())

        base_ts = np.clip(base_ts, range_start.timestamp(), end.timestamp())
        return base_ts

    def _timestamps_to_series(self, ts_array: np.ndarray) -> pd.Series:
        dti = pd.to_datetime(ts_array, unit="s", utc=True)
        return pd.Series(dti).dt.tz_localize(None)

    def generate_column(self, col_def: Dict, n: int,
                        start_dt: datetime, end_dt: datetime,
                        temporal_cfg: Dict,
                        fk_cache: Dict[str, np.ndarray],
                        intro_offset_days: int = 0,
                        orphaned_fk_pct: float = 0.0) -> pd.Series:
        ct = col_def["type"].lower()

        if ct == "foreign_key":
            ref_entity, ref_col = col_def["ref"].split(".")
            valid_keys = fk_cache.get(ref_entity)
            if valid_keys is None or len(valid_keys) == 0:
                vals = np.array([str(self.fake.uuid4()) for _ in range(n)], dtype=object)
                return pd.Series(vals)

            vals           = np.random.choice(valid_keys, size=n).astype(object)
            orphan_mask    = np.random.random(n) < orphaned_fk_pct
            orphan_count   = orphan_mask.sum()
            if orphan_count > 0:
                vals[orphan_mask] = [str(self.fake.uuid4()) for _ in range(orphan_count)]
            return pd.Series(vals)

        if ct in ("timestamp",):
            ts = self._random_timestamps(n, start_dt, end_dt, temporal_cfg, intro_offset_days, col_def.get("pattern", ""))
            s = self._timestamps_to_series(ts)
            return s

        if ct == "date":
            ts = self._random_timestamps(n, start_dt, end_dt, temporal_cfg, intro_offset_days, col_def.get("pattern", ""))
            s = self._timestamps_to_series(ts)
            return pd.Series(s.dt.date.values)

        if ct == "date_str":
            ts = self._random_timestamps(n, start_dt, end_dt, temporal_cfg, intro_offset_days, col_def.get("pattern", ""))
            s = self._timestamps_to_series(ts)
            fmt = col_def.get("format", "%Y-%m-%d")
            return pd.Series(s.dt.strftime(fmt).values)

        if ct == "integer":
            lo, hi = col_def.get("min", 0), col_def.get("max", 1000)
            return pd.Series(np.random.randint(lo, hi + 1, size=n), dtype="Int64")

        if ct == "float":
            lo, hi   = col_def.get("min", 0.0), col_def.get("max", 1000.0)
            prec     = col_def.get("precision", 2)
            raw      = np.random.uniform(lo, hi, size=n)
            return pd.Series(np.round(raw, prec))

        pool = _make_pool(self.fake, ct, col_def, n_pool=min(POOL_SIZE, n + 1))
        idx  = np.random.randint(0, len(pool), size=n)
        return pd.Series(pool[idx])

    def apply_messiness(self, df: pd.DataFrame,
                        entity_cfg: Dict,
                        global_mess: Dict,
                        pk_col: Optional[str],
                        fk_cache: Dict[str, np.ndarray]) -> pd.DataFrame:
        local_mess = entity_cfg.get("messiness", {})

        def _rate(key: str) -> float:
            return local_mess.get(key, global_mess.get(key, 0.0))

        n = len(df)
        null_pct = _rate("null_pct")
        nullable_cols = [c["name"] for c in entity_cfg.get("columns", []) if c.get("nullable", False)]
        for col in nullable_cols:
            if col in df.columns:
                null_mask = np.random.random(n) < null_pct
                df.loc[null_mask, col] = None

        dup_pct = _rate("dup_pk_pct")
        if pk_col and pk_col in df.columns and dup_pct > 0:
            dup_count = max(1, int(n * dup_pct))
            dup_indices = np.random.choice(n, size=dup_count, replace=False)
            src_indices = np.random.choice(n, size=dup_count, replace=False)
            df.iloc[dup_indices, df.columns.get_loc(pk_col)] = df.iloc[src_indices][pk_col].values

        neg_pct = _rate("negative_numeric_pct")
        if neg_pct > 0:
            num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if pk_col and pk_col in num_cols:
                num_cols.remove(pk_col)
            for col in num_cols:
                neg_mask = np.random.random(n) < neg_pct
                df.loc[neg_mask, col] = -df.loc[neg_mask, col].abs()

        imp_pct = _rate("impossible_date_pct")
        if imp_pct > 0:
            date_cols = [(c["name"], c.get("type")) for c in entity_cfg.get("columns", [])
                         if c.get("type") in ("date", "date_str", "timestamp")]
            for col, c_type in date_cols:
                if col in df.columns:
                    imp_mask = np.random.random(n) < imp_pct
                    if c_type == "date_str":
                        f_val = "2099-12-31"
                        p_val = "1900-01-01"
                    elif c_type == "date":
                        f_val = datetime(2099, 12, 31).date()
                        p_val = datetime(1900, 1, 1).date()
                    else:
                        f_val = pd.Timestamp("2099-12-31")
                        p_val = pd.Timestamp("1900-01-01")
                        
                    future_mask = imp_mask.copy()
                    future_mask[imp_mask] = np.random.random(imp_mask.sum()) > 0.5
                    past_mask = imp_mask & ~future_mask
                    
                    if future_mask.any():
                        df.loc[future_mask, col] = f_val
                    if past_mask.any():
                        df.loc[past_mask, col] = p_val

        versions = entity_cfg.get("schema_versions")
        if versions:
            temporal_cols = [c["name"] for c in entity_cfg.get("columns", []) if c.get("temporal")]
            if temporal_cols:
                base_col = temporal_cols[0]
                if base_col in df.columns:
                    temp_dates = pd.to_datetime(df[base_col], errors="coerce")
                    for v in versions:
                        v_from_str = v.get("from")
                        if not v_from_str:
                            continue
                        v_from = pd.to_datetime(v_from_str)
                        added = v.get("added_fields", [])
                        if added:
                            mask = temp_dates < v_from
                            for c in added:
                                if c in df.columns:
                                    df.loc[mask, c] = pd.NA

        return df

    def build_fk_cache(self, entity_name: str, df: pd.DataFrame,
                       pk_col: Optional[str], fk_cache: Dict[str, np.ndarray],
                       max_cache: int = 500000):
        if pk_col and pk_col in df.columns:
            vals = df[pk_col].dropna().unique()
            if len(vals) > max_cache:
                vals = np.random.choice(vals, size=max_cache, replace=False)
            fk_cache[entity_name] = vals.astype(object)

    def generate_entity(self, entity_cfg: Dict, fk_cache: Dict[str, np.ndarray],
                        global_mess: Dict) -> pd.DataFrame:
        name        = str(entity_cfg["name"])
        total_rows  = int(entity_cfg["row_count"])
        chunk_size  = int(entity_cfg.get("chunk_size", total_rows))
        intro_offset = int(entity_cfg.get("entity_intro_days_offset", 0))
        orphan_pct   = float(entity_cfg.get("messiness", {}).get(
            "orphaned_fk_pct", global_mess.get("orphaned_fk_pct", 0.0)))

        start_dt, end_dt, temporal_cfg = self._temporal_config()
        pk_col = next((c["name"] for c in entity_cfg["columns"] if c.get("primary_key")), None)

        chunks  = []
        n_done: int = 0
        chunk_i = 0

        while n_done < total_rows:
            n: int = min(chunk_size, total_rows - n_done)
            chunk_i += 1

            chunk_data = {}
            for col_def in entity_cfg["columns"]:
                col_name = col_def["name"]
                series = self.generate_column(
                    col_def       = col_def,
                    n             = n,
                    start_dt      = start_dt,
                    end_dt        = end_dt,
                    temporal_cfg  = temporal_cfg,
                    fk_cache      = fk_cache,
                    intro_offset_days = intro_offset,
                    orphaned_fk_pct   = orphan_pct,
                )
                chunk_data[col_name] = series.values

            chunk_df = pd.DataFrame(chunk_data)
            chunk_df = self.apply_messiness(
                df          = chunk_df,
                entity_cfg  = entity_cfg,
                global_mess = global_mess,
                pk_col      = pk_col,
                fk_cache    = fk_cache,
            )
            chunks.append(chunk_df)
            n_done += n

        df = pd.concat(chunks, ignore_index=True) if len(chunks) > 1 else chunks[0]
        return df

    def _pg_type(self, col_def: Dict) -> str:
        ct = col_def["type"].lower()
        mapping = {
            "uuid":       "TEXT",
            "string":     "TEXT",
            "name":       "TEXT",
            "company":    "TEXT",
            "email":      "TEXT",
            "phone":      "TEXT",
            "address":    "TEXT",
            "country":    "TEXT",
            "city":       "TEXT",
            "text":       "TEXT",
            "product_name": "TEXT",
            "job_title":  "TEXT",
            "state":      "TEXT",
            "ipv4":       "TEXT",
            "choice":     "TEXT",
            "boolean":    "BOOLEAN",
            "integer":    "INTEGER",
            "float":      "DOUBLE PRECISION",
            "timestamp":  "TIMESTAMP",
            "date":       "TEXT",
            "date_str":   "TEXT",
            "foreign_key":"TEXT",
        }
        return mapping.get(ct, "TEXT")

    def _build_create_table_sql(self, entity_cfg: Dict) -> str:
        table  = entity_cfg["name"]
        cols   = []
        seen   = set()

        for col in entity_cfg["columns"]:
            pg_type = self._pg_type(col)
            col_name = col["name"]
            if col_name not in seen:
                cols.append(f'  "{col_name}" {pg_type}')
                seen.add(col_name)

        return f'CREATE TABLE IF NOT EXISTS "{table}" (\n' + ",\n".join(cols) + "\n);"

    def load_postgres(self, df: pd.DataFrame, entity_cfg: Dict) -> int:
        """Bulk-load a DataFrame into Postgres using COPY.
        
        Optimisations applied per session:
          • synchronous_commit = off  – skip WAL disk flush (safe for bulk loads)
          • checkpoint_completion_target hint via work_mem
        For large tables the DataFrame is streamed in 200 k-row chunks so we
        never build a multi-GB in-memory CSV string.
        
        In incremental mode, the table is assumed to already exist and rows
        are appended without re-creating the table.
        """
        if not PSYCOPG2_AVAILABLE:
            return 0

        CHUNK_ROWS = 200_000          # rows per COPY command
        table = entity_cfg["name"]
        try:
            conn = psycopg2.connect(**self.pg_conn_params)
            cur  = conn.cursor()

            # ── session-level bulk-load optimisations ──────────────────────
            cur.execute("SET synchronous_commit = off;")
            cur.execute("SET work_mem = '256MB';")
            conn.commit()

            # ── create table (skip in incremental mode) ───────────────────
            if not self.incremental:
                create_sql = self._build_create_table_sql(entity_cfg)
                cur.execute(create_sql)
                conn.commit()

            cols        = list(df.columns)
            quoted_cols = [f'"{ c}"' for c in cols]
            cols_str    = ", ".join(quoted_cols)
            copy_sql    = f'COPY "{table}" ({cols_str}) FROM STDIN WITH CSV NULL AS \'\\N\''

            # ── stream in chunks ───────────────────────────────────────────
            total_chunks = max(1, len(df) // CHUNK_ROWS + (1 if len(df) % CHUNK_ROWS else 0))
            for chunk_i in range(total_chunks):
                chunk = df.iloc[chunk_i * CHUNK_ROWS : (chunk_i + 1) * CHUNK_ROWS]
                buf   = StringIO()
                chunk[cols].to_csv(buf, index=False, header=False, na_rep="\\N")
                buf.seek(0)
                cur.copy_expert(copy_sql, buf)
                del buf
            conn.commit()

            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            row = cur.fetchone()
            db_count: int = int(row[0]) if row else 0

            cur.close()
            conn.close()
            mode_label = "appended (incremental)" if self.incremental else "loaded"
            print(f"  ✓ {table}: {db_count:,} rows {mode_label}")
            return db_count
        except Exception as e:
            print(f"PostgreSQL load failed for '{table}': {e}")
            return 0

    def _get_date_sequence(self, n_files: int, frequency: str) -> List[datetime]:
        start_dt, end_dt, _ = self._temporal_config()
        dates = []
        for i in range(n_files):
            if frequency == "daily":
                d = start_dt + timedelta(days=i)
            elif frequency == "weekly":
                d = start_dt + timedelta(weeks=i)
            else:
                d = start_dt + timedelta(days=i)
            dates.append(d)
        return dates

    def generate_files(self) -> None:
        for file_cfg in self.schema.get("file_sources", []):
            name       = file_cfg["name"]
            fmt        = file_cfg["output_format"]
            out_dir    = self.output_dir / file_cfg["output_dir"]
            frequency  = file_cfg.get("frequency", "daily")
            n_files    = file_cfg["num_files"]
            dates      = self._get_date_sequence(n_files, frequency)

            for i, date_dt in enumerate(dates):
                n = file_cfg["rows_per_file"]
                start_dt, end_dt, temporal_cfg = self._temporal_config()
                chunk_data = {}
                for col_def in file_cfg["columns"]:
                    col_name = col_def["name"]
                    series   = self.generate_column(
                        col_def          = col_def,
                        n                = n,
                        start_dt         = date_dt,
                        end_dt           = min(date_dt + timedelta(days=1), end_dt),
                        temporal_cfg     = temporal_cfg,
                        fk_cache         = {},
                        orphaned_fk_pct  = 0.0,
                    )
                    chunk_data[col_name] = series.values

                df = pd.DataFrame(chunk_data)
                global_mess = self.schema.get("global_messiness", {})
                df = self.apply_messiness(
                    df          = df,
                    entity_cfg  = file_cfg,
                    global_mess = global_mess,
                    pk_col      = next((c["name"] for c in file_cfg["columns"] if c.get("primary_key")), None),
                    fk_cache    = {},
                )
                
                date_str = date_dt.strftime("%Y-%m-%d")
                pattern = file_cfg["filename_pattern"]
                filename = pattern.format(date=date_str, week=i + 1)
                filepath = out_dir / filename
                filepath.parent.mkdir(parents=True, exist_ok=True)

                local_mess = file_cfg.get("messiness", {})

                if fmt == "csv":
                    header = True
                    if local_mess.get("missing_header_pct", 0) > np.random.random():
                        header = False
                    if local_mess.get("column_order_drift", False) and np.random.random() < 0.3:
                        cols = list(df.columns)
                        random.shuffle(cols)
                        df = df[cols]
                    if local_mess.get("mixed_date_formats", False) and np.random.random() < 0.5:
                        date_cols = df.select_dtypes(include=['datetime']).columns
                        if not date_cols.empty:
                            c = random.choice(date_cols)
                            fmt_str = random.choice(["%d/%m/%Y", "%m-%d-%Y", "%Y%m%d"])
                            df[c] = df[c].dt.strftime(fmt_str)
                    # Incremental: append to existing CSV
                    if self.incremental and filepath.exists():
                        df.to_csv(filepath, mode='a', index=False, header=False)
                    else:
                        df.to_csv(filepath, index=False, header=header)
                elif fmt == "excel":
                    if not OPENPYXL_AVAILABLE: continue
                    if local_mess.get("summary_rows", False) and len(df) > 0:
                        summary_row: Dict[str, Any] = {c: None for c in df.columns}
                        num_cols = df.select_dtypes(include=[np.number]).columns
                        if not num_cols.empty:
                            for c in num_cols: summary_row[c] = df[c].sum()
                            summary_row[df.columns[0]] = "Total"
                        df = pd.concat([df, pd.DataFrame([summary_row])], ignore_index=True)
                    sheet = file_cfg.get("sheet_name", "Sheet1")
                    # Incremental: read existing Excel and append new rows
                    if self.incremental and filepath.exists():
                        existing_df = pd.read_excel(filepath, sheet_name=sheet, engine="openpyxl")
                        df = pd.concat([existing_df, df], ignore_index=True)
                    if local_mess.get("multi_sheet_split", False) and len(df) > 2:
                        split_idx = len(df) // 2
                        df1 = df.iloc[:split_idx]
                        df2 = df.iloc[split_idx:]
                        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                            df1.to_excel(writer, index=False, sheet_name=f"{sheet}_1")
                            df2.to_excel(writer, index=False, sheet_name=f"{sheet}_2")
                    else:
                        df.to_excel(filepath, index=False, sheet_name=sheet, engine="openpyxl")
                elif fmt == "parquet":
                    if not PYARROW_AVAILABLE: continue
                    if local_mess.get("schema_evolution", False) and np.random.random() < 0.2:
                        drop_candidates = [c["name"] for c in file_cfg["columns"] if not c.get("primary_key")]
                        if drop_candidates:
                            drop_c = random.choice(drop_candidates)
                            if drop_c in df.columns:
                                df = df.drop(columns=[drop_c])
                    # Incremental: read existing parquet and append new rows
                    if self.incremental and filepath.exists():
                        existing_table = pq.read_table(str(filepath))
                        existing_df = existing_table.to_pandas()
                        df = pd.concat([existing_df, df], ignore_index=True)
                    table_pa = pa.Table.from_pandas(df, preserve_index=False)
                    pq.write_table(table_pa, str(filepath))
                elif fmt == "json":
                    for col in df.select_dtypes(include=['datetime']).columns:
                        df[col] = df[col].astype(str)
                    records = cast(List[Dict[str, Any]], df.to_dict(orient="records"))
                    if local_mess.get("nested_structure_depth_variation", False):
                        for rec in records:
                            if np.random.random() < 0.2 and len(rec) >= 3:
                                all_keys: List[str] = list(rec.keys())
                                start_idx = max(0, len(all_keys) - 2)
                                nest_keys: List[str] = [all_keys[i] for i in range(start_idx, len(all_keys))]
                                rec["details"] = {k: rec.pop(k, None) for k in nest_keys}
                    if local_mess.get("null_vs_missing_keys", False):
                        for rec in records:
                            if np.random.random() < 0.3:
                                keys_to_del = [k for k, v in rec.items() if pd.isna(v) or v is None or v == "NaT"]
                                for k in keys_to_del:
                                    rec.pop(k, None)
                    # Incremental: read existing JSON array and extend
                    if self.incremental and filepath.exists():
                        with open(filepath, "r", encoding="utf-8") as fr:
                            existing_records = json.load(fr)
                        if isinstance(existing_records, list):
                            records = existing_records + records
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(records, f, indent=2, default=str)

                size_kb = float(filepath.stat().st_size) / 1024.0
                self.summary["files_generated"].append({
                    "entity": name,
                    "format": fmt,
                    "filename": str(filepath),
                    "rows": len(df),
                    "size_kb": math.floor(size_kb * 10) / 10.0,
                })
                self.summary["total_records"] += len(df)

    def generate_api_dump(self) -> None:
        for api_cfg in self.schema.get("api_dumps", []):
            name       = api_cfg["name"]
            out_dir    = self.output_dir / api_cfg["output_dir"]
            new_count  = api_cfg["total_records"]
            page_size  = api_cfg["page_size"]
            pattern    = api_cfg["filename_pattern"]

            start_dt, end_dt, temporal_cfg = self._temporal_config()
            global_mess = self.schema.get("global_messiness", {})
            out_dir.mkdir(parents=True, exist_ok=True)

            # ── Collect existing records when in incremental mode ──────────
            existing_records: List[Dict[str, Any]] = []
            if self.incremental:
                page_num = 1
                while True:
                    existing_path = out_dir / pattern.format(page=page_num)
                    if not existing_path.exists():
                        break
                    with open(existing_path, "r", encoding="utf-8") as fr:
                        page_data = json.load(fr)
                    existing_records.extend(page_data.get("data", []))
                    page_num += 1

            # ── Generate new records ───────────────────────────────────────
            new_records: List[Dict[str, Any]] = []
            gen_done = 0
            while gen_done < new_count:
                n = min(page_size, new_count - gen_done)
                chunk_data = {}
                for col_def in api_cfg["columns"]:
                    col_name = col_def["name"]
                    series   = self.generate_column(
                        col_def         = col_def,
                        n               = n,
                        start_dt        = start_dt,
                        end_dt          = end_dt,
                        temporal_cfg    = temporal_cfg,
                        fk_cache        = {},
                        orphaned_fk_pct = 0.0,
                    )
                    chunk_data[col_name] = series.values

                df = pd.DataFrame(chunk_data)
                df = self.apply_messiness(
                    df          = df,
                    entity_cfg  = api_cfg,
                    global_mess = global_mess,
                    pk_col      = None,
                    fk_cache    = {},
                )

                local_mess = api_cfg.get("messiness", {})
                soft_delete_pct = local_mess.get("soft_delete_pct", 0.0)
                if soft_delete_pct > 0:
                    mask = np.random.random(len(df)) < soft_delete_pct
                    df["is_deleted"] = mask
                
                stale_pct = local_mess.get("stale_watermark_pct", 0.0)
                if stale_pct > 0:
                    ts_cols = [c["name"] for c in api_cfg.get("columns", []) if c.get("type") in ("timestamp", "date", "date_str")]
                    if ts_cols and ts_cols[-1] in df.columns:
                        col = ts_cols[-1]
                        mask = np.random.random(len(df)) < stale_pct
                        stale_offsets = pd.to_timedelta(np.random.randint(1, 72, size=mask.sum()), unit='h')
                        df.loc[mask, col] = pd.to_datetime(df.loc[mask, col]) - stale_offsets

                for col in df.select_dtypes(include=['datetime64']).columns:
                    df[col] = df[col].astype(str)

                new_records.extend(cast(List[Dict[str, Any]], df.to_dict(orient="records")))
                gen_done += n

            # ── Merge & re-paginate ────────────────────────────────────────
            all_records = existing_records + new_records
            total       = len(all_records)
            n_pages     = max(1, math.ceil(total / page_size))

            for page in range(1, n_pages + 1):
                page_records = all_records[(page - 1) * page_size : page * page_size]
                payload = {
                    "api":           name,
                    "page":          page,
                    "page_size":     page_size,
                    "total_pages":   n_pages,
                    "total_records": total,
                    "data":          page_records,
                }
                filename = pattern.format(page=page)
                filepath = out_dir / filename
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(payload, f, default=str, indent=2)

            total_size_kb = float(sum(f.stat().st_size for f in out_dir.glob("*.json"))) / 1024.0
            self.summary["api_dumps_generated"].append({
                "name":       name,
                "pages":      n_pages,
                "records":    total,
                "size_kb":    math.floor(total_size_kb * 10) / 10.0,
            })
            self.summary["total_records"] += total

    def _topological_sort(self, entities: List[Dict]) -> List[Dict]:
        entity_map = {e["name"]: e for e in entities}
        deps: Dict[str, Set[str]] = {str(e["name"]): set() for e in entities}
        for entity in entities:
            for col in entity.get("columns", []):
                if col["type"].lower() == "foreign_key":
                    ref_entity = col["ref"].split(".")[0]
                    if ref_entity in deps:
                        cast(Set[str], deps[str(entity["name"])]).add(str(ref_entity))

        in_degree_map = {str(n): int(len(d)) for n, d in deps.items()}
        queue     = sorted(
            [n for n, d in in_degree_map.items() if d == 0],
            key=lambda n: entity_map[n].get("generation_order", 99)
        )
        sorted_names = []

        while queue:
            node = queue.pop(0)
            sorted_names.append(node)
            for other, other_deps in deps.items():
                if node in other_deps:
                    other_deps.discard(node)
                    val = in_degree_map.get(other, 0) - 1
                    in_degree_map.update({other: val})
                    if val == 0:
                        queue.append(other)
                        queue.sort(key=lambda n: entity_map[n].get("generation_order", 99))

        for n in entity_map:
            if n not in sorted_names:
                sorted_names.append(n)

        return [entity_map[n] for n in sorted_names]

    def run(self) -> Dict:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        global_mess   = self.schema.get("global_messiness", {})
        fk_max_cache  = self.schema.get("fk_cache", {}).get("sample_size", 500_000)

        db_entities_cfg = self.schema.get("database", {}).get("entities", [])

        # ── Incremental: override row counts ───────────────────────────────
        if self.incremental and self.incremental_rows > 0:
            for entity in db_entities_cfg:
                entity["row_count"] = self.incremental_rows
            for file_cfg in self.schema.get("file_sources", []):
                file_cfg["rows_per_file"] = self.incremental_rows
            for api_cfg in self.schema.get("api_dumps", []):
                api_cfg["total_records"] = self.incremental_rows

        sorted_entities  = self._topological_sort(db_entities_cfg)

        fk_cache: Dict[str, np.ndarray] = {}

        # ── Build dependency graph for parallelism ─────────────────────────
        # Entities whose FK refs are all already in fk_cache can be loaded
        # concurrently with the next independent batch.
        def _deps(entity: Dict) -> set:
            deps_set: set = set()
            for col in entity.get("columns", []):
                if col["type"].lower() == "foreign_key":
                    deps_set.add(col["ref"].split(".")[0])
            return deps_set

        # Process in waves: a wave is a batch of entities whose deps are
        # all already done.  Within a wave, generate+load in parallel.
        done_names: set = set()
        remaining = list(sorted_entities)

        def _process_entity(entity_cfg: Dict) -> tuple:
            """Generate + load one entity; returns (name, df, actual_rows)."""
            name        = entity_cfg["name"]
            target_rows = entity_cfg["row_count"]
            df = self.generate_entity(entity_cfg, fk_cache, global_mess)
            actual_rows = self.load_postgres(df, entity_cfg)
            return name, entity_cfg, df, actual_rows, target_rows

        while remaining:
            # Pick all entities whose deps are satisfied
            wave = [e for e in remaining if _deps(e).issubset(done_names)]
            if not wave:
                # Circular dep or something unexpected – fall back to sequential
                wave = [remaining[0]]

            for e in wave:
                remaining.remove(e)

            if len(wave) == 1:
                # No parallelism needed for a single entity
                name, entity_cfg, df, actual_rows, target_rows = _process_entity(wave[0])
                pk_col = next((c["name"] for c in entity_cfg["columns"] if c.get("primary_key")), None)
                self.build_fk_cache(name, df, pk_col, fk_cache, max_cache=fk_max_cache)
                self.summary["database_tables"][name] = {
                    "target_rows": target_rows,
                    "actual_rows": actual_rows if actual_rows > 0 else len(df),
                }
                self.summary["total_records"] += len(df)
                done_names.add(name)
                del df
            else:
                # Parallel wave
                max_workers = min(len(wave), 4)   # cap at 4 concurrent DB conns
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = {pool.submit(_process_entity, e): e for e in wave}  # type: ignore[arg-type]
                    for fut in as_completed(futures):
                        name, entity_cfg, df, actual_rows, target_rows = fut.result()
                        pk_col = next((c["name"] for c in entity_cfg["columns"] if c.get("primary_key")), None)
                        self.build_fk_cache(name, df, pk_col, fk_cache, max_cache=fk_max_cache)
                        self.summary["database_tables"][name] = {
                            "target_rows": target_rows,
                            "actual_rows": actual_rows if actual_rows > 0 else len(df),
                        }
                        self.summary["total_records"] += len(df)
                        done_names.add(name)
                        del df

        self.generate_files()
        self.generate_api_dump()

        self.summary["execution_seconds"] = math.floor(float(time.time() - self.start_time) * 100) / 100.0

        out_path = self.output_dir / "summary.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(self.summary, f, indent=2, default=str)

        return self.summary
