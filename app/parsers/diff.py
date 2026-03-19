"""
Diff generation module for comparing parsed data between PDF versions.
Compares new version's records against the currently active version's records.
Returns a JSON-serializable summary showing record counts, value changes, and samples.
"""

import json
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import func as sqla_func

from app.database import (
    PdfVersion,
    LocalMultiplier,
    CurrentCostMultiplier,
    StoryHeightMultiplier,
    FloorAreaPerimeterMultiplier,
    SprinklerCost,
    HvacCost,
    BaseCostTable,
    BaseCostRow,
    ElevatorType,
    ElevatorCost,
    ElevatorCostPerStop,
)


def get_active_version_id(db: Session) -> Optional[int]:
    """Get the ID of the currently active PDF version"""
    active = db.query(PdfVersion).filter(PdfVersion.is_active == True).first()
    return active.id if active else None


def _pct_change(old_val: float, new_val: float) -> Optional[float]:
    """Calculate percentage change, returns None if old is 0"""
    if old_val == 0:
        return None
    return round(((new_val - old_val) / old_val) * 100, 2)


def _sample_changes(old_rows: List[Dict], new_rows: List[Dict], key_fields: List[str], value_fields: List[str], max_samples: int = 5) -> List[Dict]:
    """
    Compare rows by key fields and find value differences.
    Returns sample changes sorted by largest absolute change.
    """
    # Index old rows by key
    old_map = {}
    for row in old_rows:
        key = tuple(row.get(k) for k in key_fields)
        old_map[key] = row

    changes = []
    for row in new_rows:
        key = tuple(row.get(k) for k in key_fields)
        old_row = old_map.get(key)
        if not old_row:
            continue

        for vf in value_fields:
            old_val = old_row.get(vf)
            new_val = row.get(vf)
            if old_val is None or new_val is None:
                continue
            old_f = float(old_val)
            new_f = float(new_val)
            if old_f != new_f:
                changes.append({
                    "key": {k: row.get(k) for k in key_fields},
                    "field": vf,
                    "old": old_f,
                    "new": new_f,
                    "change": round(new_f - old_f, 4),
                    "pct_change": _pct_change(old_f, new_f),
                })

    # Sort by absolute change magnitude (largest first)
    changes.sort(key=lambda c: abs(c.get("pct_change") or 0), reverse=True)
    return changes[:max_samples]


def generate_diff_local_multipliers(db: Session, new_version_id: int) -> Dict:
    """Generate diff for local multipliers between new version and active version"""
    active_id = get_active_version_id(db)

    new_count = db.query(LocalMultiplier).filter(LocalMultiplier.pdf_version_id == new_version_id).count()
    old_count = db.query(LocalMultiplier).filter(LocalMultiplier.pdf_version_id == active_id).count() if active_id else 0

    result = {
        "old_version_id": active_id,
        "new_version_id": new_version_id,
        "old_count": old_count,
        "new_count": new_count,
        "count_delta": new_count - old_count,
        "is_first_version": active_id is None,
        "sample_changes": [],
    }

    if not active_id or old_count == 0:
        return result

    # Get sample value changes for common locations
    old_rows = db.query(LocalMultiplier).filter(LocalMultiplier.pdf_version_id == active_id).all()
    new_rows = db.query(LocalMultiplier).filter(LocalMultiplier.pdf_version_id == new_version_id).all()

    old_dicts = [{"location": r.location, "region": r.region, "class_a": r.class_a, "class_b": r.class_b, "class_c": r.class_c, "class_d": r.class_d, "class_s": r.class_s} for r in old_rows]
    new_dicts = [{"location": r.location, "region": r.region, "class_a": r.class_a, "class_b": r.class_b, "class_c": r.class_c, "class_d": r.class_d, "class_s": r.class_s} for r in new_rows]

    result["sample_changes"] = _sample_changes(
        old_dicts, new_dicts,
        key_fields=["location", "region"],
        value_fields=["class_a", "class_b", "class_c", "class_d", "class_s"],
    )
    return result


def generate_diff_current_cost(db: Session, new_version_id: int) -> Dict:
    """Generate diff for current cost multipliers"""
    active_id = get_active_version_id(db)

    new_count = db.query(CurrentCostMultiplier).filter(CurrentCostMultiplier.pdf_version_id == new_version_id).count()
    old_count = db.query(CurrentCostMultiplier).filter(CurrentCostMultiplier.pdf_version_id == active_id).count() if active_id else 0

    result = {
        "old_version_id": active_id,
        "new_version_id": new_version_id,
        "old_count": old_count,
        "new_count": new_count,
        "count_delta": new_count - old_count,
        "is_first_version": active_id is None,
        "sample_changes": [],
    }

    if not active_id or old_count == 0:
        return result

    old_rows = db.query(CurrentCostMultiplier).filter(CurrentCostMultiplier.pdf_version_id == active_id).all()
    new_rows = db.query(CurrentCostMultiplier).filter(CurrentCostMultiplier.pdf_version_id == new_version_id).all()

    old_dicts = [{"method": r.method, "region": r.region, "building_class": r.building_class, "effective_date": r.effective_date, "multiplier": r.multiplier} for r in old_rows]
    new_dicts = [{"method": r.method, "region": r.region, "building_class": r.building_class, "effective_date": r.effective_date, "multiplier": r.multiplier} for r in new_rows]

    result["sample_changes"] = _sample_changes(
        old_dicts, new_dicts,
        key_fields=["method", "region", "building_class", "effective_date"],
        value_fields=["multiplier"],
    )
    return result


def generate_diff_story_height(db: Session, new_version_id: int, section: int) -> Dict:
    """Generate diff for story height multipliers for a specific section"""
    active_id = get_active_version_id(db)

    new_count = db.query(StoryHeightMultiplier).filter(
        StoryHeightMultiplier.pdf_version_id == new_version_id,
        StoryHeightMultiplier.section == section
    ).count()
    old_count = db.query(StoryHeightMultiplier).filter(
        StoryHeightMultiplier.pdf_version_id == active_id,
        StoryHeightMultiplier.section == section
    ).count() if active_id else 0

    result = {
        "old_version_id": active_id,
        "new_version_id": new_version_id,
        "section": section,
        "old_count": old_count,
        "new_count": new_count,
        "count_delta": new_count - old_count,
        "is_first_version": active_id is None,
        "sample_changes": [],
    }

    if not active_id or old_count == 0:
        return result

    old_rows = db.query(StoryHeightMultiplier).filter(StoryHeightMultiplier.pdf_version_id == active_id, StoryHeightMultiplier.section == section).all()
    new_rows = db.query(StoryHeightMultiplier).filter(StoryHeightMultiplier.pdf_version_id == new_version_id, StoryHeightMultiplier.section == section).all()

    old_dicts = [{"height_feet": r.height_feet, "sqft_multiplier": r.sqft_multiplier, "cuft_multiplier": r.cuft_multiplier} for r in old_rows]
    new_dicts = [{"height_feet": r.height_feet, "sqft_multiplier": r.sqft_multiplier, "cuft_multiplier": r.cuft_multiplier} for r in new_rows]

    result["sample_changes"] = _sample_changes(
        old_dicts, new_dicts,
        key_fields=["height_feet"],
        value_fields=["sqft_multiplier", "cuft_multiplier"],
    )
    return result


def generate_diff_floor_area_perimeter(db: Session, new_version_id: int, section: int) -> Dict:
    """Generate diff for floor area/perimeter multipliers for a specific section"""
    active_id = get_active_version_id(db)

    new_count = db.query(FloorAreaPerimeterMultiplier).filter(
        FloorAreaPerimeterMultiplier.pdf_version_id == new_version_id,
        FloorAreaPerimeterMultiplier.section == section
    ).count()
    old_count = db.query(FloorAreaPerimeterMultiplier).filter(
        FloorAreaPerimeterMultiplier.pdf_version_id == active_id,
        FloorAreaPerimeterMultiplier.section == section
    ).count() if active_id else 0

    result = {
        "old_version_id": active_id,
        "new_version_id": new_version_id,
        "section": section,
        "old_count": old_count,
        "new_count": new_count,
        "count_delta": new_count - old_count,
        "is_first_version": active_id is None,
        "sample_changes": [],
    }

    if not active_id or old_count == 0:
        return result

    old_rows = db.query(FloorAreaPerimeterMultiplier).filter(FloorAreaPerimeterMultiplier.pdf_version_id == active_id, FloorAreaPerimeterMultiplier.section == section).all()
    new_rows = db.query(FloorAreaPerimeterMultiplier).filter(FloorAreaPerimeterMultiplier.pdf_version_id == new_version_id, FloorAreaPerimeterMultiplier.section == section).all()

    old_dicts = [{"floor_area_sqft": r.floor_area_sqft, "perimeter_ft": r.perimeter_ft, "multiplier": r.multiplier} for r in old_rows]
    new_dicts = [{"floor_area_sqft": r.floor_area_sqft, "perimeter_ft": r.perimeter_ft, "multiplier": r.multiplier} for r in new_rows]

    result["sample_changes"] = _sample_changes(
        old_dicts, new_dicts,
        key_fields=["floor_area_sqft", "perimeter_ft"],
        value_fields=["multiplier"],
    )
    return result


def generate_diff_base_cost_tables(db: Session, new_version_id: int, section: int) -> Dict:
    """Generate diff for base cost tables for a specific section"""
    active_id = get_active_version_id(db)

    new_tables = db.query(BaseCostTable).filter(BaseCostTable.pdf_version_id == new_version_id, BaseCostTable.section == section).all()
    old_tables = db.query(BaseCostTable).filter(BaseCostTable.pdf_version_id == active_id, BaseCostTable.section == section).all() if active_id else []

    new_row_count = sum(len(t.rows) for t in new_tables)
    old_row_count = sum(len(t.rows) for t in old_tables)

    result = {
        "old_version_id": active_id,
        "new_version_id": new_version_id,
        "section": section,
        "old_count": len(old_tables),
        "new_count": len(new_tables),
        "count_delta": len(new_tables) - len(old_tables),
        "old_row_count": old_row_count,
        "new_row_count": new_row_count,
        "row_count_delta": new_row_count - old_row_count,
        "is_first_version": active_id is None,
        "sample_changes": [],
    }

    if not active_id or len(old_tables) == 0:
        return result

    # Compare cost values for matching tables by file_name
    old_map = {t.file_name: t for t in old_tables if t.file_name}
    changes = []
    for new_t in new_tables:
        if not new_t.file_name or new_t.file_name not in old_map:
            continue
        old_t = old_map[new_t.file_name]

        # Index old rows by (building_class, quality_type)
        old_row_map = {}
        for r in old_t.rows:
            key = (r.building_class, r.quality_type)
            old_row_map[key] = r

        for nr in new_t.rows:
            key = (nr.building_class, nr.quality_type)
            old_r = old_row_map.get(key)
            if not old_r:
                continue
            if nr.cost_sqft and old_r.cost_sqft:
                old_v = float(old_r.cost_sqft)
                new_v = float(nr.cost_sqft)
                if old_v != new_v:
                    changes.append({
                        "key": {"table": new_t.name, "class": nr.building_class, "type": nr.quality_type},
                        "field": "cost_sqft",
                        "old": old_v,
                        "new": new_v,
                        "change": round(new_v - old_v, 2),
                        "pct_change": _pct_change(old_v, new_v),
                    })

    changes.sort(key=lambda c: abs(c.get("pct_change") or 0), reverse=True)
    result["sample_changes"] = changes[:5]
    return result


# ============ DISPATCHER ============

def generate_diff(db: Session, new_version_id: int, parser_name: str) -> str:
    """
    Generate a diff summary for a parser run and return as JSON string.
    Dispatches to the appropriate diff generator based on parser_name.
    """
    diff = None

    if parser_name == "local_multipliers":
        diff = generate_diff_local_multipliers(db, new_version_id)
    elif parser_name == "current_cost":
        diff = generate_diff_current_cost(db, new_version_id)
    elif parser_name.startswith("story_height_s"):
        section = int(parser_name.replace("story_height_s", ""))
        diff = generate_diff_story_height(db, new_version_id, section)
    elif parser_name.startswith("floor_area_perimeter_s"):
        section = int(parser_name.replace("floor_area_perimeter_s", ""))
        diff = generate_diff_floor_area_perimeter(db, new_version_id, section)
    elif parser_name.startswith("base_cost_tables_s"):
        section = int(parser_name.replace("base_cost_tables_s", ""))
        diff = generate_diff_base_cost_tables(db, new_version_id, section)
    else:
        # For parsers without specific diff logic (sprinklers, hvac, elevators),
        # return a basic count-only diff
        diff = {"is_first_version": True, "old_count": 0, "new_count": 0, "count_delta": 0, "sample_changes": []}

    if diff is None:
        diff = {"error": f"No diff generator for parser: {parser_name}"}

    return json.dumps(diff, default=str)
