"""
schema_adapter.py
-----------------
Converts the user-friendly "unified entities" YAML format into the
internal generator format understood by DataGenerator.

Supported user-format top-level keys
-------------------------------------
  project, version
  global_settings:
    temporal_range: ["YYYY-MM-DD", "YYYY-MM-DD"]
    fk_cache_enabled: bool
  entities:
    - name: ...
      source_type: database | file | api_dump
      ...

The adapter is idempotent: if the schema already has the internal
keys (temporal, database, global_messiness) it is returned unchanged.
"""

from __future__ import annotations
import re
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def adapt_schema(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Return a schema in the internal generator format."""
    # Already in internal format – pass through unchanged
    if "temporal" in raw and "database" in raw and "global_messiness" in raw:
        return raw

    out: Dict[str, Any] = {
        "project": raw.get("project", "unnamed_project"),
        "version": str(raw.get("version", "1.0.0")),
    }

    # ── temporal ──────────────────────────────────────────────────────────
    gs = raw.get("global_settings", {})
    tr = gs.get("temporal_range", ["2023-01-01", "2024-12-31"])
    out["temporal"] = {
        "start_date": str(tr[0]),
        "end_date":   str(tr[1]),
    }

    # ── fk_cache ──────────────────────────────────────────────────────────
    out["fk_cache"] = {"enabled": bool(gs.get("fk_cache_enabled", True))}

    # ── global_messiness ──────────────────────────────────────────────────
    # Allow a top-level global_messiness block in the user schema too
    user_gm = raw.get("global_messiness", {})
    out["global_messiness"] = {
        "null_pct": _pct(user_gm.get("null_pct", gs.get("default_null_pct", 0.04))),
    }

    # ── split entities by source_type ─────────────────────────────────────
    entities: List[Dict] = raw.get("entities", [])

    db_entities:  List[Dict] = []
    file_sources: List[Dict] = []
    api_dumps:    List[Dict] = []

    for e in entities:
        src = str(e.get("source_type", "database")).lower()
        if src == "database":
            db_entities.append(_adapt_db_entity(e))
        elif src == "file":
            file_sources.append(_adapt_file_source(e))
        elif src in ("api_dump", "api"):
            api_dumps.append(_adapt_api_dump(e))

    out["database"]     = {"entities": db_entities}
    out["file_sources"] = file_sources
    out["api_dumps"]    = api_dumps

    return out


# ---------------------------------------------------------------------------
# Per-entity adapters
# ---------------------------------------------------------------------------

def _adapt_db_entity(e: Dict) -> Dict:
    """Convert a database source_type entity to the internal DB entity format."""
    name       = e.get("target", e["name"])        # prefer explicit target table name
    row_count  = int(e.get("volume", 0) or e.get("row_count", 100))
    columns    = [_adapt_column(c) for c in e.get("columns", [])]
    messiness  = _adapt_messiness(e.get("messiness", {}))

    out: Dict[str, Any] = {
        "name":      name,
        "row_count": row_count,
        "columns":   columns,
    }
    if messiness:
        out["messiness"] = messiness
    # Forward any extra keys the generator understands
    for k in ("chunk_size", "entity_intro_days_offset", "schema_versions", "generation_order"):
        if k in e:
            out[k] = e[k]
    return out


def _adapt_file_source(e: Dict) -> Dict:
    """Convert a file source_type entity to the internal file_sources format."""
    name       = e["name"]
    fmt        = str(e.get("format", "csv")).lower()
    volume     = int(e.get("volume", 0) or e.get("row_count", 1000))
    num_files  = int(e.get("num_files", 0) or max(1, volume // 500))
    rows_per   = int(e.get("rows_per_file", 0) or volume // max(1, num_files))
    frequency  = str(e.get("frequency", "daily"))
    columns    = [_adapt_column(c) for c in e.get("columns", [])]
    messiness  = _adapt_messiness(e.get("messiness", {}))

    # derive output_dir and filename_pattern from path_structure if present
    path_struct = e.get("path_structure", "")
    if path_struct:
        # e.g. "exports/shipments/year={year}/month={month}/"
        # Keep only path segments that contain no template variables
        segments = path_struct.replace("\\", "/").split("/")
        static_segments = [s for s in segments if "{" not in s and "=" not in s and s]
        output_dir = "/".join(static_segments) if static_segments else name
        ext            = {"parquet": "parquet", "excel": "xlsx", "json": "json"}.get(fmt, "csv")
        filename_pattern = e.get("filename_pattern", f"{name}_{{date}}.{ext}")
    else:
        output_dir       = e.get("output_dir", name)
        ext              = {"parquet": "parquet", "excel": "xlsx", "json": "json"}.get(fmt, "csv")
        filename_pattern = e.get("filename_pattern", f"{name}_{{date}}.{ext}")

    out: Dict[str, Any] = {
        "name":             name,
        "output_format":    fmt,
        "output_dir":       output_dir,
        "filename_pattern": filename_pattern,
        "num_files":        num_files,
        "rows_per_file":    rows_per,
        "frequency":        frequency,
        "columns":          columns,
    }
    if messiness:
        out["messiness"] = messiness
    if "sheet_name" in e:
        out["sheet_name"] = e["sheet_name"]
    return out


def _adapt_api_dump(e: Dict) -> Dict:
    """Convert an api_dump source_type entity to the internal api_dumps format."""
    name         = e["name"]
    total        = int(e.get("approx_record_count", 0) or e.get("total_records", 1000))
    rs           = e.get("response_structure", {})
    page_size    = int(rs.get("page_size", e.get("page_size", 100)))
    output_dir   = e.get("output_dir", name)
    filename_pat = e.get("filename_pattern", "page_{page}.json")

    # Columns can come from "fields" (user format) or "columns" (internal)
    raw_cols  = e.get("fields") or e.get("columns") or []
    columns   = [_adapt_column(c) for c in raw_cols]
    messiness = _adapt_messiness(e.get("messiness", {}))

    out: Dict[str, Any] = {
        "name":             name,
        "output_dir":       output_dir,
        "total_records":    total,
        "page_size":        page_size,
        "filename_pattern": filename_pat,
        "columns":          columns,
    }
    if messiness:
        out["messiness"] = messiness
    return out


# ---------------------------------------------------------------------------
# Column adapter
# ---------------------------------------------------------------------------

def _adapt_column(c: Dict) -> Dict:
    """Normalise a single column definition."""
    out = dict(c)  # shallow copy

    raw_type = str(c.get("type", "string")).lower()

    # type aliases
    type_map = {
        "decimal":  "float",
        "numeric":  "float",
        "number":   "float",
        "double":   "float",
        "real":     "float",
        "int":      "integer",
        "bigint":   "integer",
        "smallint": "integer",
        "varchar":  "string",
        "text":     "string",
        "char":     "string",
        "enum":     "choice",
        "bool":     "boolean",
        "datetime": "timestamp",
        "person":   "name",
    }
    out["type"] = type_map.get(raw_type, raw_type)

    # enum → choice
    if c.get("type", "").lower() == "enum" and "values" in c:
        out["choices"] = c["values"]
        out.pop("values", None)

    # range → min/max
    if "range" in c:
        rng = c["range"]
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            out["min"] = rng[0]
            out["max"] = rng[1]
        out.pop("range", None)

    # logical_link → foreign_key ref
    if "logical_link" in c:
        link = c["logical_link"]          # e.g. "warehouses_table.warehouse_id"
        out["type"] = "foreign_key"
        out["ref"]  = link
        out.pop("logical_link", None)

    # regex pattern → simple generator pattern (best-effort)
    if "pattern" in out and out["type"] == "string":
        out["pattern"] = _regex_to_simple(str(out["pattern"]))

    # nullable_pct on column → mark column as nullable
    # The actual injection rate is handled by global/entity messiness
    if "nullable_pct" in c:
        out["nullable"] = True
        # Store as a hint (generator applies null_pct from messiness to nullable cols)
        out.pop("nullable_pct", None)

    return out


# ---------------------------------------------------------------------------
# Messiness adapter
# ---------------------------------------------------------------------------

def _adapt_messiness(m: Dict) -> Dict:
    """Convert user messiness keys to internal generator messiness keys."""
    if not m:
        return {}
    out: Dict[str, Any] = {}

    # null rate
    if "null_pct" in m:
        out["null_pct"] = _pct(m["null_pct"])
    if "null_rate" in m:
        out["null_pct"] = _pct(m["null_rate"])

    # duplicate PKs
    for k in ("duplicate_rate", "dup_pk_pct", "duplicate_pct"):
        if k in m:
            out["dup_pk_pct"] = _pct(m[k])
            break

    # impossible values → map to generator flags
    if m.get("impossible_values"):
        out["negative_numeric_pct"] = 0.02
        out["impossible_date_pct"]  = 0.01

    # naming inconsistencies → column order drift (closest approximation)
    if m.get("naming_inconsistencies"):
        out["column_order_drift"] = True

    # forward any already-internal keys
    for k in ("orphaned_fk_pct", "missing_header_pct", "mixed_date_formats",
              "multi_sheet_split", "summary_rows", "schema_evolution",
              "nested_structure_depth_variation", "null_vs_missing_keys",
              "soft_delete_pct", "stale_watermark_pct"):
        if k in m:
            out[k] = m[k]

    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(val: Any) -> float:
    """Normalise a percentage: values > 1 are treated as whole-number %."""
    f = float(val)
    return f / 100.0 if f > 1.0 else f


def _regex_to_simple(pattern: str) -> str:
    """
    Best-effort conversion of a basic regex pattern to the generator's
    simple pattern syntax (? = uppercase letter, # = digit, * = alphanumeric).

    Handles the most common notations seen in the user's YAML:
      ^WH-[A-Z]{2}-[0-9]{3}$  →  WH-??-###
      1Z[A-Z0-9]{12}           →  1Z************
    """
    # Strip anchors
    p = pattern.lstrip("^").rstrip("$")

    result: List[str] = []
    i: int = 0
    plen: int = len(p)
    while i < plen:
        ch: str = p[i]
        if ch == "[":
            end = p.find("]", i)
            if end == -1:
                result.append(ch)
                i += 1
                continue
            char_class = p[i+1:end]
            # peek at quantifier
            j = end + 1
            qty = 1
            if j < len(p) and p[j] == "{":
                close = p.find("}", j)
                if close != -1:
                    try:
                        qty = int(p[j+1:close])
                        j = close + 1
                    except ValueError:
                        pass
            elif j < len(p) and p[j] in ("+", "*"):
                qty = 8  # arbitrary default for unbounded
                j += 1
            elif j < len(p) and p[j] == "?":
                qty = 0
                j += 1

            # map class to pattern char
            has_upper  = "A-Z" in char_class
            has_lower  = "a-z" in char_class
            has_digit  = "0-9" in char_class
            if (has_upper or has_lower) and has_digit:
                sym = "*"
            elif has_upper or has_lower:
                sym = "?"
            elif has_digit:
                sym = "#"
            else:
                sym = "?"

            result.append(sym * qty)
            i = j
        elif ch == "\\":
            # escaped char – just emit the next char literally
            if i + 1 < len(p):
                result.append(p[i+1])
                i += 2
            else:
                i += 1
        elif ch in ("+", "*", "?", "."):
            result.append("*")
            i += 1
        else:
            result.append(ch)
            i += 1

    return "".join(result)
