"""
schema_validator.py
-------------------
Comprehensive YAML schema validator.

Validates the user-supplied YAML against the expected structure
before handing it off to the generator.  Returns a list of errors
(fatal) and warnings (non-fatal).

Both the "user-friendly unified" format **and** the internal
generator format are supported.
"""

from __future__ import annotations
import re
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Known column types (after type-alias mapping)
# ---------------------------------------------------------------------------

_KNOWN_TYPES = frozenset({
    # Core
    "uuid", "string", "integer", "float", "boolean",
    # Date / time
    "timestamp", "date", "date_str",
    # Faker-powered
    "name", "person", "company", "email", "phone",
    "address", "country", "city", "text", "product_name",
    "job_title", "state", "ipv4",
    # Special
    "choice", "foreign_key",
})

# Type aliases the adapter accepts (maps onto the known types above)
_TYPE_ALIASES = frozenset({
    "decimal", "numeric", "number", "double", "real",
    "int", "bigint", "smallint", "bigserial", "serial",
    "varchar", "text", "char",
    "enum", "bool",
    "datetime", "full_name", "ip",
})

_ALL_VALID_TYPES = _KNOWN_TYPES | _TYPE_ALIASES

# Supported file output formats
_VALID_FILE_FORMATS = {"csv", "json", "parquet", "excel"}

# Supported frequencies
_VALID_FREQUENCIES = {"daily", "weekly", "monthly"}

# Supported source_type values for user-friendly format
_VALID_SOURCE_TYPES = {"database", "file", "api_dump", "api"}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_schema(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a parsed YAML schema dict.

    Returns::

        {
            "valid": bool,
            "errors": [{"path": "...", "message": "..."}],
            "warnings": [{"path": "...", "message": "..."}],
            "summary": { "entity_count": int, "column_count": int }
        }
    """
    errors:   List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []

    is_internal = _is_internal_format(raw)

    # Top-level checks
    _check_project(raw, errors, warnings)
    _check_temporal(raw, errors, warnings, is_internal)

    entity_count = 0
    column_count = 0

    if is_internal:
        # Internal generator format
        ec, cc = _validate_internal_format(raw, errors, warnings)
        entity_count += ec
        column_count += cc
    else:
        # User-friendly unified format
        ec, cc = _validate_user_format(raw, errors, warnings)
        entity_count += ec
        column_count += cc

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "summary": {
            "entity_count": entity_count,
            "column_count": column_count,
        },
    }


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def _is_internal_format(raw: Dict) -> bool:
    return "temporal" in raw and "database" in raw and "global_messiness" in raw


# ---------------------------------------------------------------------------
# Top-level checks
# ---------------------------------------------------------------------------

def _check_project(raw: Dict, errors: List, warnings: List) -> None:
    if "project" not in raw:
        warnings.append({
            "path": "project",
            "message": "No 'project' name specified — will default to 'unnamed_project'.",
        })

def _check_temporal(raw: Dict, errors: List, warnings: List, is_internal: bool) -> None:
    if is_internal:
        t = raw.get("temporal")
        if not t:
            errors.append({"path": "temporal", "message": "Missing required 'temporal' block."})
            return
        _validate_date_range(t, "temporal", errors)
    else:
        gs = raw.get("global_settings", {})
        tr = gs.get("temporal_range")
        # Also support direct temporal block in user-friendly format
        if raw.get("temporal"):
            t = raw["temporal"]
            _validate_date_range(t, "temporal", errors)
        elif tr:
            if not isinstance(tr, (list, tuple)):
                errors.append({"path": "global_settings.temporal_range", "message": "temporal_range must be a list of two dates [start, end]."})
            elif len(tr) != 2:
                errors.append({"path": "global_settings.temporal_range", "message": f"temporal_range must have exactly 2 elements, got {len(tr)}."})
            else:
                _validate_date_str(str(tr[0]), "global_settings.temporal_range[0]", errors)
                _validate_date_str(str(tr[1]), "global_settings.temporal_range[1]", errors)
        else:
            warnings.append({
                "path": "temporal / global_settings.temporal_range",
                "message": "No temporal range specified — defaults to 2023-01-01 ~ 2024-12-31.",
            })


def _validate_date_range(t: Dict, path: str, errors: List) -> None:
    sd = t.get("start_date")
    ed = t.get("end_date")
    if not sd:
        errors.append({"path": f"{path}.start_date", "message": "Missing start_date."})
    else:
        _validate_date_str(str(sd), f"{path}.start_date", errors)
    if not ed:
        errors.append({"path": f"{path}.end_date", "message": "Missing end_date."})
    else:
        _validate_date_str(str(ed), f"{path}.end_date", errors)


def _validate_date_str(value: str, path: str, errors: List) -> None:
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        errors.append({"path": path, "message": f"Invalid date format '{value}'. Expected YYYY-MM-DD."})


# ---------------------------------------------------------------------------
# Internal format validation
# ---------------------------------------------------------------------------

def _validate_internal_format(raw: Dict, errors: List, warnings: List) -> Tuple[int, int]:
    entity_count = 0
    column_count = 0

    # database.entities
    db = raw.get("database", {})
    if not isinstance(db, dict):
        errors.append({"path": "database", "message": "'database' must be a mapping."})
    else:
        entities = db.get("entities", [])
        if not isinstance(entities, list):
            errors.append({"path": "database.entities", "message": "'database.entities' must be a list."})
        else:
            all_entity_names: List[str] = []
            for i, ent in enumerate(entities):
                p = f"database.entities[{i}]"
                ec, cc = _validate_entity(ent, p, errors, warnings, "database")
                entity_count += ec
                column_count += cc
                if "name" in ent:
                    all_entity_names.append(ent["name"])
            # Check FK references
            _check_fk_references(entities, "database.entities", errors, all_entity_names)

    # file_sources
    fs = raw.get("file_sources", [])
    if fs and not isinstance(fs, list):
        errors.append({"path": "file_sources", "message": "'file_sources' must be a list."})
    elif isinstance(fs, list):
        for i, f_ent in enumerate(fs):
            p = f"file_sources[{i}]"
            ec, cc = _validate_file_source_internal(f_ent, p, errors, warnings)
            entity_count += ec
            column_count += cc

    # api_dumps
    ad = raw.get("api_dumps", [])
    if ad and not isinstance(ad, list):
        errors.append({"path": "api_dumps", "message": "'api_dumps' must be a list."})
    elif isinstance(ad, list):
        for i, a_ent in enumerate(ad):
            p = f"api_dumps[{i}]"
            ec, cc = _validate_api_dump_internal(a_ent, p, errors, warnings)
            entity_count += ec
            column_count += cc

    # global_messiness
    gm = raw.get("global_messiness")
    if gm and not isinstance(gm, dict):
        errors.append({"path": "global_messiness", "message": "'global_messiness' must be a mapping."})
    elif isinstance(gm, dict):
        _validate_messiness(gm, "global_messiness", warnings)

    return entity_count, column_count


# ---------------------------------------------------------------------------
# User-friendly format validation
# ---------------------------------------------------------------------------

def _validate_user_format(raw: Dict, errors: List, warnings: List) -> Tuple[int, int]:
    entity_count = 0
    column_count = 0

    entities = raw.get("entities", [])

    # Also support the direct "database.entities" style in user format
    if not entities and "database" in raw:
        db = raw.get("database", {})
        if isinstance(db, dict):
            entities = db.get("entities", [])

    if not isinstance(entities, list):
        errors.append({"path": "entities", "message": "'entities' must be a list."})
        return 0, 0

    if len(entities) == 0:
        warnings.append({"path": "entities", "message": "No entities defined — nothing will be generated."})
        return 0, 0

    all_entity_names: List[str] = []
    db_entities_for_fk: List[Dict] = []

    for i, ent in enumerate(entities):
        p = f"entities[{i}]"
        if not isinstance(ent, dict):
            errors.append({"path": p, "message": "Each entity must be a mapping."})
            continue

        src = str(ent.get("source_type", "database")).lower()
        if src not in _VALID_SOURCE_TYPES:
            errors.append({
                "path": f"{p}.source_type",
                "message": f"Unknown source_type '{src}'. Must be one of: {', '.join(sorted(_VALID_SOURCE_TYPES))}.",
            })

        if src == "database":
            ec, cc = _validate_entity(ent, p, errors, warnings, "database")
            entity_count += ec
            column_count += cc
            db_entities_for_fk.append(ent)
        elif src == "file":
            ec, cc = _validate_entity(ent, p, errors, warnings, "file")
            entity_count += ec
            column_count += cc
            _validate_file_specific(ent, p, errors, warnings)
        elif src in ("api_dump", "api"):
            ec, cc = _validate_entity(ent, p, errors, warnings, "api")
            entity_count += ec
            column_count += cc
            _validate_api_specific(ent, p, errors, warnings)

        if "name" in ent:
            all_entity_names.append(ent.get("target", ent["name"]))

    # FK reference checks (only within database entities)
    _check_fk_references(db_entities_for_fk, "entities", errors, all_entity_names)

    # global_messiness (also accepted at top level in user format)
    gm = raw.get("global_messiness")
    if gm and isinstance(gm, dict):
        _validate_messiness(gm, "global_messiness", warnings)

    return entity_count, column_count


# ---------------------------------------------------------------------------
# Entity validation
# ---------------------------------------------------------------------------

def _validate_entity(ent: Dict, path: str, errors: List, warnings: List, kind: str) -> Tuple[int, int]:
    if not isinstance(ent, dict):
        errors.append({"path": path, "message": "Entity must be a mapping."})
        return 0, 0

    # name
    name = ent.get("name")
    if not name:
        errors.append({"path": f"{path}.name", "message": "Entity is missing required 'name' field."})
    elif not isinstance(name, str):
        errors.append({"path": f"{path}.name", "message": "Entity 'name' must be a string."})
    elif not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        warnings.append({"path": f"{path}.name", "message": f"Entity name '{name}' contains unusual characters. Recommend using snake_case identifiers."})

    # row_count / volume
    row_count = ent.get("row_count") or ent.get("volume")
    if kind == "database":
        if not row_count:
            warnings.append({"path": f"{path}.row_count", "message": f"No row_count specified for entity '{name}' — defaults to 100."})
        elif not isinstance(row_count, (int, float)):
            errors.append({"path": f"{path}.row_count", "message": f"row_count must be a number, got '{type(row_count).__name__}'."})
        elif int(row_count) <= 0:
            errors.append({"path": f"{path}.row_count", "message": "row_count must be greater than 0."})
        elif int(row_count) > 10_000_000:
            warnings.append({"path": f"{path}.row_count", "message": f"row_count of {int(row_count):,} is very large and may take a long time to generate."})

    # columns / fields
    columns = ent.get("columns") or ent.get("fields") or []
    if not columns:
        errors.append({"path": f"{path}.columns", "message": f"Entity '{name or '?'}' has no columns defined."})
        return 1, 0

    if not isinstance(columns, list):
        errors.append({"path": f"{path}.columns", "message": "'columns' must be a list."})
        return 1, 0

    col_names_seen: List[str] = []
    has_pk = False
    col_count = 0

    for ci, col in enumerate(columns):
        cp = f"{path}.columns[{ci}]"
        if not isinstance(col, dict):
            errors.append({"path": cp, "message": "Each column must be a mapping."})
            continue

        col_count += 1

        # column name
        col_name = col.get("name")
        if not col_name:
            errors.append({"path": f"{cp}.name", "message": "Column is missing required 'name' field."})
        elif col_name in col_names_seen:
            errors.append({"path": f"{cp}.name", "message": f"Duplicate column name '{col_name}' in entity '{name or '?'}'."})
        else:
            col_names_seen.append(col_name)

        # column type
        col_type = col.get("type")
        if not col_type:
            errors.append({"path": f"{cp}.type", "message": f"Column '{col_name or '?'}' is missing required 'type' field."})
        elif str(col_type).lower() not in _ALL_VALID_TYPES:
            errors.append({
                "path": f"{cp}.type",
                "message": f"Unknown column type '{col_type}'. See docs for supported types.",
            })

        # primary_key
        if col.get("primary_key"):
            has_pk = True

        # choice type needs choices/values
        ct_lower = str(col_type).lower() if col_type else ""
        if ct_lower in ("choice", "enum"):
            choices = col.get("choices") or col.get("values")
            if not choices:
                errors.append({
                    "path": f"{cp}.choices",
                    "message": f"Column '{col_name or '?'}' of type '{col_type}' must have 'choices' or 'values' list.",
                })
            elif not isinstance(choices, list) or len(choices) == 0:
                errors.append({
                    "path": f"{cp}.choices",
                    "message": f"'choices'/'values' must be a non-empty list.",
                })

        # foreign_key must have ref or logical_link
        if ct_lower == "foreign_key":
            ref = col.get("ref")
            if not ref:
                errors.append({
                    "path": f"{cp}.ref",
                    "message": f"Column '{col_name or '?'}' of type 'foreign_key' must have a 'ref' field (entity.column).",
                })
            elif "." not in str(ref):
                errors.append({
                    "path": f"{cp}.ref",
                    "message": f"FK ref '{ref}' must be in format 'entity_name.column_name'.",
                })

        # logical_link validation
        if "logical_link" in col:
            link = col["logical_link"]
            if "." not in str(link):
                errors.append({
                    "path": f"{cp}.logical_link",
                    "message": f"logical_link '{link}' must be in format 'entity_name.column_name'.",
                })

        # integer/float range checks
        if ct_lower in ("integer", "int", "bigint", "smallint", "serial", "bigserial"):
            lo = col.get("min")
            hi = col.get("max")
            rng = col.get("range")
            if rng and isinstance(rng, (list, tuple)):
                lo, hi = rng[0], rng[1] if len(rng) >= 2 else None
            if lo is not None and hi is not None:
                try:
                    if float(lo) > float(hi):
                        errors.append({
                            "path": f"{cp}.min/max",
                            "message": f"Column '{col_name}': min ({lo}) is greater than max ({hi}).",
                        })
                except (TypeError, ValueError):
                    pass

        if ct_lower in ("float", "decimal", "numeric", "number", "double", "real"):
            lo = col.get("min")
            hi = col.get("max")
            rng = col.get("range")
            if rng and isinstance(rng, (list, tuple)):
                lo, hi = rng[0], rng[1] if len(rng) >= 2 else None
            if lo is not None and hi is not None:
                try:
                    if float(lo) > float(hi):
                        errors.append({
                            "path": f"{cp}.min/max",
                            "message": f"Column '{col_name}': min ({lo}) is greater than max ({hi}).",
                        })
                except (TypeError, ValueError):
                    pass

    if kind == "database" and not has_pk:
        warnings.append({
            "path": f"{path}",
            "message": f"Entity '{name or '?'}' has no primary_key column. Consider adding one for referential integrity.",
        })

    # messiness block
    mess = ent.get("messiness", {})
    if mess and isinstance(mess, dict):
        _validate_messiness(mess, f"{path}.messiness", warnings)

    return 1, col_count


# ---------------------------------------------------------------------------
# File source validation
# ---------------------------------------------------------------------------

def _validate_file_specific(ent: Dict, path: str, errors: List, warnings: List) -> None:
    fmt = str(ent.get("format", ent.get("output_format", "csv"))).lower()
    if fmt not in _VALID_FILE_FORMATS:
        errors.append({
            "path": f"{path}.format",
            "message": f"Unknown file format '{fmt}'. Supported: {', '.join(sorted(_VALID_FILE_FORMATS))}.",
        })

    freq = str(ent.get("frequency", "daily")).lower()
    if freq not in _VALID_FREQUENCIES:
        warnings.append({
            "path": f"{path}.frequency",
            "message": f"Unknown frequency '{freq}'. Supported: {', '.join(sorted(_VALID_FREQUENCIES))}.",
        })


def _validate_file_source_internal(ent: Dict, path: str, errors: List, warnings: List) -> Tuple[int, int]:
    ec, cc = _validate_entity(ent, path, errors, warnings, "file")

    fmt = str(ent.get("output_format", "csv")).lower()
    if fmt not in _VALID_FILE_FORMATS:
        errors.append({
            "path": f"{path}.output_format",
            "message": f"Unknown output_format '{fmt}'. Supported: {', '.join(sorted(_VALID_FILE_FORMATS))}.",
        })

    nf = ent.get("num_files")
    if nf is not None and (not isinstance(nf, (int, float)) or int(nf) <= 0):
        errors.append({"path": f"{path}.num_files", "message": "num_files must be a positive integer."})

    rpf = ent.get("rows_per_file")
    if rpf is not None and (not isinstance(rpf, (int, float)) or int(rpf) <= 0):
        errors.append({"path": f"{path}.rows_per_file", "message": "rows_per_file must be a positive integer."})

    return ec, cc


# ---------------------------------------------------------------------------
# API dump validation
# ---------------------------------------------------------------------------

def _validate_api_specific(ent: Dict, path: str, errors: List, warnings: List) -> None:
    total = ent.get("approx_record_count") or ent.get("total_records")
    if not total:
        warnings.append({
            "path": f"{path}.approx_record_count",
            "message": "No record count specified — defaults to 1000.",
        })


def _validate_api_dump_internal(ent: Dict, path: str, errors: List, warnings: List) -> Tuple[int, int]:
    ec, cc = _validate_entity(ent, path, errors, warnings, "api")

    total = ent.get("total_records")
    if not total:
        warnings.append({"path": f"{path}.total_records", "message": "No total_records specified — defaults to 1000."})
    elif not isinstance(total, (int, float)) or int(total) <= 0:
        errors.append({"path": f"{path}.total_records", "message": "total_records must be a positive number."})

    ps = ent.get("page_size")
    if ps is not None and (not isinstance(ps, (int, float)) or int(ps) <= 0):
        errors.append({"path": f"{path}.page_size", "message": "page_size must be a positive integer."})

    return ec, cc


# ---------------------------------------------------------------------------
# FK reference cross-check
# ---------------------------------------------------------------------------

def _check_fk_references(entities: List[Dict], root_path: str, errors: List, all_names: List[str]) -> None:
    """Check that foreign_key 'ref' points to an existing entity."""
    for i, ent in enumerate(entities):
        columns = ent.get("columns") or ent.get("fields") or []
        for ci, col in enumerate(columns):
            ct = str(col.get("type", "")).lower()
            ref = col.get("ref") or col.get("logical_link")
            if ct == "foreign_key" and ref and "." in str(ref):
                ref_entity = str(ref).split(".")[0]
                if ref_entity not in all_names:
                    name = ent.get("name", "?")
                    errors.append({
                        "path": f"{root_path}[{i}].columns[{ci}].ref",
                        "message": f"FK reference '{ref}' in entity '{name}' points to unknown entity '{ref_entity}'. Available entities: {', '.join(all_names) if all_names else '(none)'}.",
                    })
            elif "logical_link" in col:
                link = col["logical_link"]
                if "." in str(link):
                    ref_entity = str(link).split(".")[0]
                    if ref_entity not in all_names:
                        name = ent.get("name", "?")
                        warnings.append({
                            "path": f"{root_path}[{i}].columns[{ci}].logical_link",
                            "message": f"logical_link '{link}' in entity '{name}' references entity '{ref_entity}' which is not defined in the schema.",
                        })


# ---------------------------------------------------------------------------
# Messiness validation
# ---------------------------------------------------------------------------

def _validate_messiness(mess: Dict, path: str, warnings: List) -> None:
    known_keys = {
        "null_pct", "null_rate",
        "duplicate_rate", "dup_pk_pct", "duplicate_pct",
        "impossible_values",
        "naming_inconsistencies",
        "orphaned_fk_pct",
        "missing_header_pct",
        "mixed_date_formats",
        "multi_sheet_split",
        "summary_rows",
        "schema_evolution",
        "nested_structure_depth_variation",
        "null_vs_missing_keys",
        "soft_delete_pct",
        "stale_watermark_pct",
        "negative_numeric_pct",
        "impossible_date_pct",
        "column_order_drift",
    }
    for k in mess:
        if k not in known_keys:
            warnings.append({
                "path": f"{path}.{k}",
                "message": f"Unknown messiness key '{k}'. It will be ignored by the generator.",
            })

    # Validate percentage values are in sensible range
    for k in ("null_pct", "null_rate", "dup_pk_pct", "duplicate_rate", "duplicate_pct",
              "orphaned_fk_pct", "missing_header_pct", "soft_delete_pct",
              "stale_watermark_pct", "negative_numeric_pct", "impossible_date_pct"):
        val = mess.get(k)
        if val is not None:
            try:
                f = float(val)
                if f < 0:
                    warnings.append({
                        "path": f"{path}.{k}",
                        "message": f"Negative percentage value ({f}) for '{k}'. Did you mean a positive value?",
                    })
                elif f > 100:
                    warnings.append({
                        "path": f"{path}.{k}",
                        "message": f"Very large value ({f}) for '{k}'. Values > 1 are treated as whole-number percentages.",
                    })
            except (TypeError, ValueError):
                warnings.append({
                    "path": f"{path}.{k}",
                    "message": f"'{k}' should be a number, got '{val}'.",
                })
