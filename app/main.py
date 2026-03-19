from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime
import os

from app.database import get_db, init_db, BaseCostTable, BaseCostRow, LocalMultiplier, CurrentCostMultiplier, StoryHeightMultiplier, FloorAreaPerimeterMultiplier, SprinklerCost, HvacCost, ElevatorType, ElevatorCost, ElevatorCostPerStop, PdfVersion, ParseRun
from app.parsers import local_multipliers, current_cost, story_height, floor_area_perimeter
from app.parsers import base_cost_tables
from app.parsers.diff import generate_diff

app = FastAPI(
    title="MVS Parser Service",
    description="Service for parsing Marshall Valuation Service PDF data and writing to PostgreSQL",
    version="1.0.0"
)

# Enable CORS for webapp access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    # Ensure tables exist
    init_db()
    print("[MVS Parser Service] Database initialized")
    
    # Run migration to fix TEXT columns
    try:
        from app.database import get_session_local
        from sqlalchemy import text
        SessionLocal = get_session_local()
        db = SessionLocal()
        db.execute(text("ALTER TABLE mvs_base_cost_rows ALTER COLUMN building_class TYPE TEXT"))
        db.execute(text("ALTER TABLE mvs_base_cost_rows ALTER COLUMN quality_type TYPE TEXT"))
        db.commit()
        db.close()
        print("[MVS Parser Service] TEXT columns migration completed")
    except Exception as e:
        print(f"[MVS Parser Service] Migration skipped or failed: {e}")


# ============ KNOWN PARSERS REGISTRY ============
# Defines ALL parsers that should run for a complete parse
KNOWN_PARSERS = [
    {"name": "local_multipliers", "label": "Local Multipliers", "description": "Location-based cost adjustment multipliers (865+ entries)", "section": None, "page_type": "range", "default_start_page": 719, "default_end_page": 724},
    {"name": "current_cost", "label": "Current Cost Multipliers", "description": "Time-based cost adjustment multipliers by region/class/date", "section": None, "page_type": "single", "default_start_page": 717, "default_end_page": None},
    {"name": "story_height_s11", "label": "Story Height - S11", "description": "Height multipliers for Apartments & Hotels (Section 11)", "section": 11, "page_type": "single", "default_start_page": 90, "default_end_page": None},
    {"name": "story_height_s13", "label": "Story Height - S13", "description": "Height multipliers for Stores & Commercial (Section 13)", "section": 13, "page_type": "single", "default_start_page": 218, "default_end_page": None},
    {"name": "story_height_s14", "label": "Story Height - S14", "description": "Height multipliers for Garages & Industrial (Section 14)", "section": 14, "page_type": "single", "default_start_page": 215, "default_end_page": None},
    {"name": "story_height_s15", "label": "Story Height - S15", "description": "Height multipliers for Offices & Medical (Section 15)", "section": 15, "page_type": "single", "default_start_page": None, "default_end_page": None},
    {"name": "floor_area_perimeter_s11", "label": "Floor Area/Perimeter - S11", "description": "Size/shape multipliers for Apartments & Hotels", "section": 11, "page_type": "csv", "default_start_page": 90, "default_end_page": None},
    {"name": "floor_area_perimeter_s13", "label": "Floor Area/Perimeter - S13", "description": "Size/shape multipliers for Stores & Commercial", "section": 13, "page_type": "csv", "default_start_page": 217, "default_end_page": None},
    {"name": "floor_area_perimeter_s14", "label": "Floor Area/Perimeter - S14", "description": "Size/shape multipliers for Garages & Industrial", "section": 14, "page_type": "csv", "default_start_page": 214, "default_end_page": 215},
    {"name": "floor_area_perimeter_s15", "label": "Floor Area/Perimeter - S15", "description": "Size/shape multipliers for Offices & Medical", "section": 15, "page_type": "csv", "default_start_page": None, "default_end_page": None},
    {"name": "base_cost_tables_s11", "label": "Base Cost Tables - S11", "description": "Base cost tables for Section 11 building types", "section": 11, "page_type": None, "default_start_page": None, "default_end_page": None},
    {"name": "base_cost_tables_s13", "label": "Base Cost Tables - S13", "description": "Base cost tables for Section 13 building types", "section": 13, "page_type": None, "default_start_page": None, "default_end_page": None},
    {"name": "base_cost_tables_s14", "label": "Base Cost Tables - S14", "description": "Base cost tables for Section 14 building types", "section": 14, "page_type": None, "default_start_page": None, "default_end_page": None},
    {"name": "base_cost_tables_s15", "label": "Base Cost Tables - S15", "description": "Base cost tables for Section 15 building types", "section": 15, "page_type": None, "default_start_page": None, "default_end_page": None},
    {"name": "sprinklers", "label": "Sprinkler Costs", "description": "Sprinkler system cost refinements (all sections)", "section": None, "page_type": None, "default_start_page": None, "default_end_page": None},
    {"name": "hvac", "label": "HVAC Costs", "description": "HVAC/climate adjustment costs (all sections)", "section": None, "page_type": None, "default_start_page": None, "default_end_page": None},
    {"name": "elevators", "label": "Elevator Costs", "description": "Elevator cost data from Section 58", "section": None, "page_type": None, "default_start_page": None, "default_end_page": None},
]


def start_parse_run(db: Session, version_id: int, parser_name: str) -> ParseRun:
    """Create or reset a parse run record and mark it as running"""
    run = db.query(ParseRun).filter(
        ParseRun.pdf_version_id == version_id,
        ParseRun.parser_name == parser_name
    ).first()
    if run:
        run.status = 'running'
        run.records_created = 0
        run.error_message = None
        run.started_at = datetime.utcnow()
        run.completed_at = None
    else:
        run = ParseRun(
            pdf_version_id=version_id,
            parser_name=parser_name,
            status='running',
            started_at=datetime.utcnow(),
        )
        db.add(run)
    db.commit()
    db.refresh(run)
    return run


def complete_parse_run(db: Session, run: ParseRun, records: int, diff_json: str = None):
    """Mark a parse run as successful, optionally storing diff summary"""
    run.status = 'success'
    run.records_created = records
    run.diff_summary = diff_json
    run.completed_at = datetime.utcnow()
    db.commit()


def fail_parse_run(db: Session, run: ParseRun, error: str):
    """Mark a parse run as failed"""
    run.status = 'failed'
    run.error_message = error[:2000]
    run.completed_at = datetime.utcnow()
    db.commit()


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "mvs-parser-service"}


@app.post("/pdf-versions/upload")
async def upload_pdf_version(
    pdf_file: UploadFile = File(...),
    version_name: str = None,
    edition_year: int = None,
    notes: str = None,
    db: Session = Depends(get_db)
):
    """Upload a PDF to the volume and register it as a version in the database"""
    import hashlib
    import time

    storage_path = os.getenv("MVS_PDF_STORAGE_PATH", "/mvs-pdfs")
    os.makedirs(storage_path, exist_ok=True)

    content = await pdf_file.read()
    file_hash = hashlib.sha256(content).hexdigest()

    # Check for duplicate by hash
    existing = db.query(PdfVersion).filter(PdfVersion.file_hash == file_hash).first()
    if existing:
        return {
            "id": existing.id,
            "version_name": existing.version_name,
            "already_existed": True,
            "message": "A PDF with this content already exists"
        }

    timestamp = int(time.time())
    safe_filename = pdf_file.filename.replace(" ", "_")
    stored_filename = f"mvs_{timestamp}_{safe_filename}"
    stored_path = os.path.join(storage_path, stored_filename)

    with open(stored_path, "wb") as f:
        f.write(content)

    version = PdfVersion(
        version_name=version_name or pdf_file.filename,
        edition_year=edition_year,
        file_size_bytes=len(content),
        file_hash=file_hash,
        storage_path=stored_path,
        original_filename=pdf_file.filename,
        is_active=False,
        is_fully_parsed=False,
        notes=notes,
    )
    db.add(version)
    db.commit()
    db.refresh(version)

    return {
        "id": version.id,
        "version_name": version.version_name,
        "edition_year": version.edition_year,
        "file_size_bytes": version.file_size_bytes,
        "file_hash": version.file_hash,
        "storage_path": version.storage_path,
        "original_filename": version.original_filename,
        "is_active": version.is_active,
        "is_fully_parsed": version.is_fully_parsed,
        "already_existed": False,
    }


@app.patch("/pdf-versions/{version_id}/activate")
async def activate_pdf_version(version_id: int, force: bool = False, db: Session = Depends(get_db)):
    """Set a version as the active version (deactivates all others).
    Validates ALL known parsers have status='success' before allowing activation.
    Pass force=true to skip validation (not recommended for production)."""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")

    # Gate: validate all parsers succeeded unless force=true
    if not force:
        runs = db.query(ParseRun).filter(ParseRun.pdf_version_id == version_id).all()
        run_map = {r.parser_name: r for r in runs}

        missing = []
        failed = []
        for p in KNOWN_PARSERS:
            run = run_map.get(p["name"])
            if not run or run.status == "not_started":
                missing.append(p["name"])
            elif run.status == "failed":
                failed.append(p["name"])
            elif run.status == "running":
                failed.append(f"{p['name']} (still running)")
            elif run.status != "success":
                failed.append(f"{p['name']} (status: {run.status})")

        if missing or failed:
            detail_parts = []
            if missing:
                detail_parts.append(f"Not run: {', '.join(missing)}")
            if failed:
                detail_parts.append(f"Failed/incomplete: {', '.join(failed)}")
            raise HTTPException(
                status_code=422,
                detail=f"Cannot activate: {'; '.join(detail_parts)}. Run all parsers successfully first, or use force=true to override."
            )

    db.query(PdfVersion).update({"is_active": False})
    version.is_active = True
    version.is_fully_parsed = True
    db.commit()
    return {"success": True, "active_version_id": version_id}


@app.patch("/pdf-versions/{version_id}/mark-parsed")
async def mark_pdf_version_parsed(version_id: int, db: Session = Depends(get_db)):
    """Mark a version as fully parsed"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")

    version.is_fully_parsed = True
    db.commit()
    return {"success": True, "version_id": version_id}


@app.post("/parse-version/{version_id}/local-multipliers")
async def parse_version_local_multipliers(version_id: int, start_page: int = None, end_page: int = None, db: Session = Depends(get_db)):
    """Parse local multipliers from a stored PDF version"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if not os.path.exists(version.storage_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found at {version.storage_path}")

    run = start_parse_run(db, version_id, "local_multipliers")
    try:
        kwargs = {"pdf_version_id": version_id}
        if start_page is not None:
            kwargs["start_page"] = start_page
        if end_page is not None:
            kwargs["end_page"] = end_page
        result = local_multipliers.parse_and_save(version.storage_path, db, **kwargs)
        diff_json = generate_diff(db, version_id, "local_multipliers")
        complete_parse_run(db, run, result, diff_json=diff_json)
        return {"success": True, "records_updated": result, "pdf_version_id": version_id}
    except Exception as e:
        fail_parse_run(db, run, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse-version/{version_id}/current-cost")
async def parse_version_current_cost(version_id: int, page: int = None, db: Session = Depends(get_db)):
    """Parse current cost multipliers from a stored PDF version"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if not os.path.exists(version.storage_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found at {version.storage_path}")

    run = start_parse_run(db, version_id, "current_cost")
    try:
        kwargs = {"pdf_version_id": version_id}
        if page is not None:
            kwargs["page"] = page
        result = current_cost.parse_and_save(version.storage_path, db, **kwargs)
        diff_json = generate_diff(db, version_id, "current_cost")
        complete_parse_run(db, run, result, diff_json=diff_json)
        return {"success": True, "records_updated": result, "pdf_version_id": version_id}
    except Exception as e:
        fail_parse_run(db, run, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse-version/{version_id}/story-height")
async def parse_version_story_height(version_id: int, section: int = 11, start_page: int = None, db: Session = Depends(get_db)):
    """Parse story height multipliers from a stored PDF version for a specific section"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if not os.path.exists(version.storage_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found at {version.storage_path}")

    parser_name = f"story_height_s{section}"
    run = start_parse_run(db, version_id, parser_name)
    try:
        page = start_page if start_page is not None else story_height.SECTION_STORY_HEIGHT_PAGES.get(section, 90)
        result = story_height.parse_and_save(version.storage_path, db, page=page, section=section, pdf_version_id=version_id)
        diff_json = generate_diff(db, version_id, parser_name)
        complete_parse_run(db, run, result, diff_json=diff_json)
        return {"success": True, "records_updated": result, "section": section, "pdf_version_id": version_id}
    except Exception as e:
        fail_parse_run(db, run, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse-version/{version_id}/story-height/all-sections")
async def parse_version_story_height_all(version_id: int, db: Session = Depends(get_db)):
    """Parse story height multipliers for ALL known sections from a stored PDF version"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if not os.path.exists(version.storage_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found at {version.storage_path}")

    section_results = {}
    for section in story_height.SECTION_STORY_HEIGHT_PAGES.keys():
        parser_name = f"story_height_s{section}"
        run = start_parse_run(db, version_id, parser_name)
        try:
            page = story_height.SECTION_STORY_HEIGHT_PAGES[section]
            result = story_height.parse_and_save(version.storage_path, db, page=page, section=section, pdf_version_id=version_id)
            diff_json = generate_diff(db, version_id, parser_name)
            complete_parse_run(db, run, result, diff_json=diff_json)
            section_results[section] = result
        except Exception as e:
            fail_parse_run(db, run, str(e))
            section_results[section] = {"error": str(e)}

    return {"success": True, "sections": section_results, "pdf_version_id": version_id}


@app.post("/parse-version/{version_id}/floor-area-perimeter")
async def parse_version_floor_area_perimeter(version_id: int, section: int = 11, pages: str = None, db: Session = Depends(get_db)):
    """Parse floor area/perimeter multipliers from a stored PDF version for a specific section.
    pages: optional comma-separated page numbers to override defaults (e.g. '214,215')"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if not os.path.exists(version.storage_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found at {version.storage_path}")

    # Override SECTION_FAP_PAGES if custom pages provided
    if pages:
        custom_pages = [int(p.strip()) for p in pages.split(',') if p.strip().isdigit()]
        if custom_pages:
            floor_area_perimeter.SECTION_FAP_PAGES[section] = custom_pages

    parser_name = f"floor_area_perimeter_s{section}"
    run = start_parse_run(db, version_id, parser_name)
    try:
        result = floor_area_perimeter.parse_and_save_section(version.storage_path, db, section=section, pdf_version_id=version_id)
        diff_json = generate_diff(db, version_id, parser_name)
        complete_parse_run(db, run, result, diff_json=diff_json)
        return {"success": True, "records_updated": result, "section": section, "pdf_version_id": version_id}
    except Exception as e:
        fail_parse_run(db, run, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse-version/{version_id}/floor-area-perimeter/all-sections")
async def parse_version_floor_area_perimeter_all(version_id: int, db: Session = Depends(get_db)):
    """Parse floor area/perimeter multipliers for ALL known sections from a stored PDF version"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if not os.path.exists(version.storage_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found at {version.storage_path}")

    section_results = {}
    for section in floor_area_perimeter.SECTION_FAP_PAGES.keys():
        parser_name = f"floor_area_perimeter_s{section}"
        run = start_parse_run(db, version_id, parser_name)
        try:
            result = floor_area_perimeter.parse_and_save_section(version.storage_path, db, section=section, pdf_version_id=version_id)
            diff_json = generate_diff(db, version_id, parser_name)
            complete_parse_run(db, run, result, diff_json=diff_json)
            section_results[section] = result
        except Exception as e:
            fail_parse_run(db, run, str(e))
            section_results[section] = {"error": str(e)}

    return {"success": True, "sections": section_results, "pdf_version_id": version_id}


@app.post("/parse-version/{version_id}/all")
async def parse_version_all(version_id: int, db: Session = Depends(get_db)):
    """Parse ALL table types from a stored PDF version in one call"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if not os.path.exists(version.storage_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found at {version.storage_path}")

    results = {}
    errors = {}

    # Parse global multipliers (not section-specific)
    for name, parser in [
        ("local_multipliers", local_multipliers),
        ("current_cost", current_cost),
    ]:
        run = start_parse_run(db, version_id, name)
        try:
            count = parser.parse_and_save(version.storage_path, db, pdf_version_id=version_id)
            diff_json = generate_diff(db, version_id, name)
            complete_parse_run(db, run, count, diff_json=diff_json)
            results[name] = count
        except Exception as e:
            fail_parse_run(db, run, str(e))
            errors[name] = str(e)
    
    # Parse section-specific story height for all known sections
    for section in story_height.SECTION_STORY_HEIGHT_PAGES.keys():
        parser_name = f"story_height_s{section}"
        run = start_parse_run(db, version_id, parser_name)
        try:
            page = story_height.SECTION_STORY_HEIGHT_PAGES[section]
            count = story_height.parse_and_save(version.storage_path, db, page=page, section=section, pdf_version_id=version_id)
            diff_json = generate_diff(db, version_id, parser_name)
            complete_parse_run(db, run, count, diff_json=diff_json)
            results[parser_name] = count
        except Exception as e:
            fail_parse_run(db, run, str(e))
            errors[parser_name] = str(e)
    
    # Parse section-specific floor area/perimeter for all known sections
    for section in floor_area_perimeter.SECTION_FAP_PAGES.keys():
        parser_name = f"floor_area_perimeter_s{section}"
        run = start_parse_run(db, version_id, parser_name)
        try:
            count = floor_area_perimeter.parse_and_save_section(version.storage_path, db, section=section, pdf_version_id=version_id)
            diff_json = generate_diff(db, version_id, parser_name)
            complete_parse_run(db, run, count, diff_json=diff_json)
            results[parser_name] = count
        except Exception as e:
            fail_parse_run(db, run, str(e))
            errors[parser_name] = str(e)

    if not errors:
        version.is_fully_parsed = True
        db.commit()

    return {
        "success": len(errors) == 0,
        "pdf_version_id": version_id,
        "results": results,
        "errors": errors,
    }


@app.post("/migrate/fix-text-columns")
async def migrate_fix_text_columns(db: Session = Depends(get_db)):
    """Migrate VARCHAR columns to TEXT for base cost rows"""
    from sqlalchemy import text
    try:
        db.execute(text("ALTER TABLE mvs_base_cost_rows ALTER COLUMN building_class TYPE TEXT"))
        db.execute(text("ALTER TABLE mvs_base_cost_rows ALTER COLUMN quality_type TYPE TEXT"))
        db.commit()
        return {"success": True, "message": "Columns migrated to TEXT"}
    except Exception as e:
        db.rollback()
        return {"success": False, "error": str(e)}


@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Get current record counts from database, with per-section breakdown for refinements"""
    from sqlalchemy import func as sqla_func
    
    # Per-section counts for story height
    sh_sections = db.query(
        StoryHeightMultiplier.section, sqla_func.count(StoryHeightMultiplier.id)
    ).group_by(StoryHeightMultiplier.section).all()
    
    # Per-section counts for floor area/perimeter
    fap_sections = db.query(
        FloorAreaPerimeterMultiplier.section, sqla_func.count(FloorAreaPerimeterMultiplier.id)
    ).group_by(FloorAreaPerimeterMultiplier.section).all()
    
    # Per-section counts for sprinklers
    spr_sections = db.query(
        SprinklerCost.section, sqla_func.count(SprinklerCost.id)
    ).group_by(SprinklerCost.section).all()
    
    # Per-section counts for HVAC
    hvac_sections = db.query(
        HvacCost.section, sqla_func.count(HvacCost.id)
    ).group_by(HvacCost.section).all()
    
    return {
        "local_multipliers": db.query(LocalMultiplier).count(),
        "current_cost_multipliers": db.query(CurrentCostMultiplier).count(),
        "story_height_multipliers": {
            "total": db.query(StoryHeightMultiplier).count(),
            "by_section": {s: c for s, c in sh_sections},
        },
        "floor_area_perimeter_multipliers": {
            "total": db.query(FloorAreaPerimeterMultiplier).count(),
            "by_section": {s: c for s, c in fap_sections},
        },
        "sprinkler_costs": {
            "total": db.query(SprinklerCost).count(),
            "by_section": {s: c for s, c in spr_sections},
        },
        "hvac_costs": {
            "total": db.query(HvacCost).count(),
            "by_section": {s: c for s, c in hvac_sections},
        },
        "base_cost_tables": db.query(BaseCostTable).count(),
        "base_cost_rows": db.query(BaseCostRow).count(),
        "elevator_types": db.query(ElevatorType).count(),
        "elevator_costs": db.query(ElevatorCost).count(),
    }


@app.post("/parse/local-multipliers")
async def parse_local_multipliers_endpoint(
    pdf_file: UploadFile = File(...),
    pdf_version_id: int = None,
    db: Session = Depends(get_db)
):
    """Parse local multipliers from uploaded PDF and update database"""
    try:
        # Save uploaded file temporarily
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        result = local_multipliers.parse_and_save(temp_path, db, pdf_version_id=pdf_version_id)
        
        # Clean up
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result, "pdf_version_id": pdf_version_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse/current-cost")
async def parse_current_cost_endpoint(
    pdf_file: UploadFile = File(...),
    pdf_version_id: int = None,
    db: Session = Depends(get_db)
):
    """Parse current cost multipliers from uploaded PDF and update database"""
    try:
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        result = current_cost.parse_and_save(temp_path, db, pdf_version_id=pdf_version_id)
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result, "pdf_version_id": pdf_version_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse/story-height")
async def parse_story_height_endpoint(
    pdf_file: UploadFile = File(...),
    section: int = 11,
    pdf_version_id: int = None,
    db: Session = Depends(get_db)
):
    """Parse story height multipliers from uploaded PDF for a specific section"""
    try:
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        page = story_height.SECTION_STORY_HEIGHT_PAGES.get(section, 90)
        result = story_height.parse_and_save(temp_path, db, page=page, section=section, pdf_version_id=pdf_version_id)
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result, "section": section, "pdf_version_id": pdf_version_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse/floor-area-perimeter")
async def parse_floor_area_perimeter_endpoint(
    pdf_file: UploadFile = File(...),
    section: int = 11,
    pdf_version_id: int = None,
    db: Session = Depends(get_db)
):
    """Parse floor area/perimeter multipliers from uploaded PDF for a specific section"""
    try:
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        result = floor_area_perimeter.parse_and_save_section(temp_path, db, section=section, pdf_version_id=pdf_version_id)
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result, "section": section, "pdf_version_id": pdf_version_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ LOCAL MULTIPLIERS ENDPOINTS ============

@app.get("/local-multipliers")
async def list_local_multipliers(
    country: str = None,
    region: str = None,
    db: Session = Depends(get_db)
):
    """List all local multipliers, optionally filtered by country or region"""
    query = db.query(LocalMultiplier)
    if country:
        query = query.filter(LocalMultiplier.country.ilike(f"%{country}%"))
    if region:
        query = query.filter(LocalMultiplier.region.ilike(f"%{region}%"))
    
    multipliers = query.order_by(LocalMultiplier.country, LocalMultiplier.region, LocalMultiplier.location).all()
    
    return {
        "count": len(multipliers),
        "multipliers": [
            {
                "id": m.id,
                "location": m.location,
                "city": m.city,
                "region": m.region,
                "country": m.country,
                "class_a": float(m.class_a) if m.class_a else None,
                "class_b": float(m.class_b) if m.class_b else None,
                "class_c": float(m.class_c) if m.class_c else None,
                "class_d": float(m.class_d) if m.class_d else None,
                "class_s": float(m.class_s) if m.class_s else None,
                "is_regional": m.is_regional,
                "source_page": m.source_page,
            }
            for m in multipliers
        ]
    }


@app.get("/local-multipliers/regions")
async def list_local_multiplier_regions(
    country: str = None,
    db: Session = Depends(get_db)
):
    """Get unique regions for local multipliers"""
    query = db.query(LocalMultiplier.region, LocalMultiplier.country).distinct()
    if country:
        query = query.filter(LocalMultiplier.country.ilike(f"%{country}%"))
    
    results = query.order_by(LocalMultiplier.country, LocalMultiplier.region).all()
    
    return {
        "regions": [{"region": r.region, "country": r.country} for r in results]
    }


# ============ CURRENT COST MULTIPLIERS ENDPOINTS ============

@app.get("/current-cost-multipliers")
async def list_current_cost_multipliers(
    method: str = None,
    region: str = None,
    building_class: str = None,
    db: Session = Depends(get_db)
):
    """List all current cost multipliers, optionally filtered"""
    query = db.query(CurrentCostMultiplier)
    if method:
        query = query.filter(CurrentCostMultiplier.method.ilike(f"%{method}%"))
    if region:
        query = query.filter(CurrentCostMultiplier.region.ilike(f"%{region}%"))
    if building_class:
        query = query.filter(CurrentCostMultiplier.building_class == building_class)
    
    multipliers = query.order_by(
        CurrentCostMultiplier.method,
        CurrentCostMultiplier.region,
        CurrentCostMultiplier.building_class,
        CurrentCostMultiplier.effective_date
    ).all()
    
    return {
        "count": len(multipliers),
        "multipliers": [
            {
                "id": m.id,
                "method": m.method,
                "region": m.region,
                "building_class": m.building_class,
                "effective_date": m.effective_date,
                "multiplier": float(m.multiplier) if m.multiplier else None,
                "source_page": m.source_page,
            }
            for m in multipliers
        ]
    }


@app.get("/current-cost-multipliers/methods")
async def list_current_cost_methods(db: Session = Depends(get_db)):
    """Get unique methods for current cost multipliers"""
    results = db.query(CurrentCostMultiplier.method).distinct().all()
    return {"methods": [r.method for r in results]}


@app.get("/current-cost-multipliers/regions")
async def list_current_cost_regions(db: Session = Depends(get_db)):
    """Get unique regions for current cost multipliers"""
    results = db.query(CurrentCostMultiplier.region).distinct().all()
    return {"regions": [r.region for r in results]}


# ============ STORY HEIGHT MULTIPLIER ENDPOINTS ============

@app.get("/story-height-multipliers")
async def list_story_height_multipliers(
    section: int = None,
    db: Session = Depends(get_db)
):
    """List story height multipliers, optionally filtered by section"""
    query = db.query(StoryHeightMultiplier)
    if section is not None:
        query = query.filter(StoryHeightMultiplier.section == section)
    
    multipliers = query.order_by(StoryHeightMultiplier.section, StoryHeightMultiplier.height_feet).all()
    
    return {
        "count": len(multipliers),
        "multipliers": [
            {
                "id": m.id,
                "section": m.section,
                "height_meters": float(m.height_meters) if m.height_meters else None,
                "height_feet": m.height_feet,
                "sqft_multiplier": float(m.sqft_multiplier) if m.sqft_multiplier else None,
                "cuft_multiplier": float(m.cuft_multiplier) if m.cuft_multiplier else None,
                "source_page": m.source_page,
            }
            for m in multipliers
        ]
    }


@app.get("/story-height-multipliers/sections")
async def list_story_height_sections(db: Session = Depends(get_db)):
    """Get sections that have story height multiplier data"""
    from sqlalchemy import func as sqla_func
    results = db.query(
        StoryHeightMultiplier.section, sqla_func.count(StoryHeightMultiplier.id)
    ).group_by(StoryHeightMultiplier.section).order_by(StoryHeightMultiplier.section).all()
    return {"sections": [{"section": s, "count": c} for s, c in results]}


# ============ FLOOR AREA / PERIMETER MULTIPLIER ENDPOINTS ============

@app.get("/floor-area-perimeter-multipliers")
async def list_floor_area_perimeter_multipliers(
    section: int = None,
    db: Session = Depends(get_db)
):
    """List floor area/perimeter multipliers, optionally filtered by section"""
    query = db.query(FloorAreaPerimeterMultiplier)
    if section is not None:
        query = query.filter(FloorAreaPerimeterMultiplier.section == section)
    
    multipliers = query.order_by(
        FloorAreaPerimeterMultiplier.section,
        FloorAreaPerimeterMultiplier.floor_area_sqft,
        FloorAreaPerimeterMultiplier.perimeter_ft
    ).all()
    
    return {
        "count": len(multipliers),
        "multipliers": [
            {
                "id": m.id,
                "section": m.section,
                "floor_area_sqft": m.floor_area_sqft,
                "perimeter_ft": m.perimeter_ft,
                "multiplier": float(m.multiplier) if m.multiplier else None,
                "source_page": m.source_page,
            }
            for m in multipliers
        ]
    }


@app.get("/floor-area-perimeter-multipliers/sections")
async def list_fap_sections(db: Session = Depends(get_db)):
    """Get sections that have floor area/perimeter multiplier data"""
    from sqlalchemy import func as sqla_func
    results = db.query(
        FloorAreaPerimeterMultiplier.section, sqla_func.count(FloorAreaPerimeterMultiplier.id)
    ).group_by(FloorAreaPerimeterMultiplier.section).order_by(FloorAreaPerimeterMultiplier.section).all()
    return {"sections": [{"section": s, "count": c} for s, c in results]}


# ============ SPRINKLER COST ENDPOINTS ============

@app.get("/sprinkler-costs")
async def list_sprinkler_costs(
    section: int = None,
    system_type: str = None,
    db: Session = Depends(get_db)
):
    """List sprinkler costs, optionally filtered by section and system type"""
    query = db.query(SprinklerCost)
    if section is not None:
        query = query.filter(SprinklerCost.section == section)
    if system_type:
        query = query.filter(SprinklerCost.system_type == system_type)
    
    costs = query.order_by(SprinklerCost.section, SprinklerCost.system_type, SprinklerCost.coverage_sqft).all()
    
    return {
        "count": len(costs),
        "costs": [
            {
                "id": c.id,
                "section": c.section,
                "system_type": c.system_type,
                "coverage_sqft": c.coverage_sqft,
                "quality_low": float(c.quality_low) if c.quality_low else None,
                "quality_avg": float(c.quality_avg) if c.quality_avg else None,
                "quality_good": float(c.quality_good) if c.quality_good else None,
                "quality_excl": float(c.quality_excl) if c.quality_excl else None,
                "source_page": c.source_page,
            }
            for c in costs
        ]
    }


@app.get("/sprinkler-costs/sections")
async def list_sprinkler_sections(db: Session = Depends(get_db)):
    """Get sections that have sprinkler cost data"""
    from sqlalchemy import func as sqla_func
    results = db.query(
        SprinklerCost.section, sqla_func.count(SprinklerCost.id)
    ).group_by(SprinklerCost.section).order_by(SprinklerCost.section).all()
    return {"sections": [{"section": s, "count": c} for s, c in results]}


# ============ HVAC COST ENDPOINTS ============

@app.get("/hvac-costs")
async def list_hvac_costs(
    section: int = None,
    category: str = None,
    db: Session = Depends(get_db)
):
    """List HVAC costs, optionally filtered by section and category"""
    query = db.query(HvacCost)
    if section is not None:
        query = query.filter(HvacCost.section == section)
    if category:
        query = query.filter(HvacCost.category == category)
    
    costs = query.order_by(HvacCost.section, HvacCost.category, HvacCost.hvac_type).all()
    
    return {
        "count": len(costs),
        "costs": [
            {
                "id": c.id,
                "section": c.section,
                "category": c.category,
                "hvac_type": c.hvac_type,
                "label": c.label,
                "cost_mild": float(c.cost_mild) if c.cost_mild else None,
                "cost_moderate": float(c.cost_moderate) if c.cost_moderate else None,
                "cost_extreme": float(c.cost_extreme) if c.cost_extreme else None,
                "source_page": c.source_page,
            }
            for c in costs
        ]
    }


@app.get("/hvac-costs/sections")
async def list_hvac_sections(db: Session = Depends(get_db)):
    """Get sections that have HVAC cost data"""
    from sqlalchemy import func as sqla_func
    results = db.query(
        HvacCost.section, sqla_func.count(HvacCost.id)
    ).group_by(HvacCost.section).order_by(HvacCost.section).all()
    return {"sections": [{"section": s, "count": c} for s, c in results]}


@app.get("/hvac-costs/categories")
async def list_hvac_categories(section: int = None, db: Session = Depends(get_db)):
    """Get available HVAC categories, optionally for a specific section"""
    query = db.query(HvacCost.section, HvacCost.category).distinct()
    if section is not None:
        query = query.filter(HvacCost.section == section)
    results = query.order_by(HvacCost.section, HvacCost.category).all()
    return {"categories": [{"section": s, "category": c} for s, c in results]}


@app.post("/import/hvac-costs")
async def import_hvac_costs(
    json_file: UploadFile = File(...),
    section: int = 11,
    db: Session = Depends(get_db)
):
    """
    Import HVAC cost data from uploaded JSON file (S11_hvac.json format).
    Clears existing data for the given section before importing.
    """
    import json as json_lib
    
    try:
        content = await json_file.read()
        data = json_lib.loads(content.decode('utf-8'))
        
        # Clear existing HVAC data for this section
        db.query(HvacCost).filter(HvacCost.section == section).delete()
        db.commit()
        
        source_page = data.get('metadata', {}).get('pdf_page', 88)
        imported = 0
        
        categories = data.get('categories', {})
        for cat_key, cat_data in categories.items():
            items = cat_data.get('items', [])
            for item in items:
                entry = HvacCost(
                    section=section,
                    category=cat_key,
                    hvac_type=item['type'],
                    label=item['label'],
                    cost_mild=item.get('mild'),
                    cost_moderate=item.get('moderate'),
                    cost_extreme=item.get('extreme'),
                    source_page=source_page,
                )
                db.add(entry)
                imported += 1
        
        db.commit()
        
        return {
            "success": True,
            "section": section,
            "imported": imported,
            "source_page": source_page,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ============ BASE COST TABLE ENDPOINTS ============

@app.get("/tables")
async def list_tables(
    section: int = None,
    db: Session = Depends(get_db)
):
    """List all base cost tables, optionally filtered by section"""
    query = db.query(BaseCostTable)
    if section is not None:
        query = query.filter(BaseCostTable.section == section)
    
    tables = query.order_by(BaseCostTable.section, BaseCostTable.page).all()
    
    return {
        "count": len(tables),
        "tables": [
            {
                "id": t.id,
                "name": t.name,
                "occupancy_code": t.occupancy_code,
                "section": t.section,
                "page": t.page,
                "pdf_page": t.pdf_page,
                "file_name": t.file_name,
                "row_count": len(t.rows)
            }
            for t in tables
        ]
    }


@app.get("/tables/{table_id}")
async def get_table(
    table_id: int,
    db: Session = Depends(get_db)
):
    """Get a single table with all its rows and metadata"""
    table = db.query(BaseCostTable).filter(BaseCostTable.id == table_id).first()
    
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    
    return {
        "id": table.id,
        "name": table.name,
        "occupancy_code": table.occupancy_code,
        "section": table.section,
        "page": table.page,
        "pdf_page": table.pdf_page,
        "notes": table.notes,
        "file_name": table.file_name,
        "rows": [
            {
                "id": r.id,
                "building_class": r.building_class,
                "quality_type": r.quality_type,
                "exterior_walls": r.exterior_walls,
                "interior_finish": r.interior_finish,
                "lighting_plumbing": r.lighting_plumbing,
                "heat": r.heat,
                "cost_sqm": float(r.cost_sqm) if r.cost_sqm else None,
                "cost_cuft": float(r.cost_cuft) if r.cost_cuft else None,
                "cost_sqft": float(r.cost_sqft) if r.cost_sqft else None,
            }
            for r in sorted(table.rows, key=lambda x: x.row_order)
        ]
    }


@app.get("/tables/by-name/{name}")
async def get_table_by_name(
    name: str,
    db: Session = Depends(get_db)
):
    """Search for tables by name (partial match)"""
    tables = db.query(BaseCostTable).filter(
        BaseCostTable.name.ilike(f"%{name}%")
    ).all()
    
    return {
        "count": len(tables),
        "tables": [
            {
                "id": t.id,
                "name": t.name,
                "occupancy_code": t.occupancy_code,
                "section": t.section,
                "page": t.page,
                "pdf_page": t.pdf_page,
            }
            for t in tables
        ]
    }


@app.post("/import/base-cost-tables")
async def import_base_cost_tables(
    section: int,
    files: list[UploadFile] = File(...),
    pdf_version_id: int = None,
    db: Session = Depends(get_db)
):
    """
    Import base cost tables from uploaded markdown files
    
    Args:
        section: Section number (used for grouping and clearing existing)
        files: List of markdown files to import
        pdf_version_id: Optional PDF version ID to associate with imported tables
    """
    try:
        import tempfile
        import shutil
        
        # Create temp directory
        temp_dir = tempfile.mkdtemp()
        
        # Save uploaded files
        for f in files:
            file_path = os.path.join(temp_dir, f.filename)
            with open(file_path, "wb") as dest:
                content = await f.read()
                dest.write(content)
        
        # Import from directory
        result = base_cost_tables.import_from_directory(temp_dir, db, section, pdf_version_id=pdf_version_id)
        
        # Clean up
        shutil.rmtree(temp_dir)
        
        result['pdf_version_id'] = pdf_version_id
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ ELEVATOR ENDPOINTS ============

@app.get("/elevators")
async def list_elevators(
    category: str = None,
    db: Session = Depends(get_db)
):
    """List all elevator types, optionally filtered by category (passenger/freight)"""
    query = db.query(ElevatorType)
    if category:
        query = query.filter(ElevatorType.category.ilike(f"%{category}%"))
    
    elevator_types = query.order_by(ElevatorType.category, ElevatorType.name).all()
    
    return {
        "count": len(elevator_types),
        "elevators": [
            {
                "id": e.id,
                "category": e.category,
                "name": e.name,
                "source_page": e.source_page,
                "cost_count": len(e.costs),
                "cost_per_stop_count": len(e.cost_per_stops),
            }
            for e in elevator_types
        ]
    }


@app.get("/elevators/{elevator_id}")
async def get_elevator(
    elevator_id: int,
    db: Session = Depends(get_db)
):
    """Get a single elevator type with all its costs"""
    elevator = db.query(ElevatorType).filter(ElevatorType.id == elevator_id).first()
    
    if not elevator:
        raise HTTPException(status_code=404, detail="Elevator type not found")
    
    # Group costs by speed to create matrix view
    costs_by_speed = {}
    for cost in elevator.costs:
        if cost.speed_fpm not in costs_by_speed:
            costs_by_speed[cost.speed_fpm] = {}
        costs_by_speed[cost.speed_fpm][cost.capacity_lbs] = float(cost.base_cost)
    
    # Get unique speeds and capacities
    speeds = sorted(set(c.speed_fpm for c in elevator.costs))
    capacities = sorted(set(c.capacity_lbs for c in elevator.costs))
    
    return {
        "id": elevator.id,
        "category": elevator.category,
        "name": elevator.name,
        "source_page": elevator.source_page,
        "speeds": speeds,
        "capacities": capacities,
        "cost_matrix": costs_by_speed,
        "costs": [
            {
                "id": c.id,
                "speed_fpm": c.speed_fpm,
                "capacity_lbs": c.capacity_lbs,
                "base_cost": float(c.base_cost),
            }
            for c in sorted(elevator.costs, key=lambda x: (x.speed_fpm, x.capacity_lbs))
        ],
        "cost_per_stop": [
            {
                "id": cps.id,
                "capacity_lbs": cps.capacity_lbs,
                "door_type": cps.door_type,
                "cost_per_stop": float(cps.cost_per_stop),
            }
            for cps in sorted(elevator.cost_per_stops, key=lambda x: (x.door_type, x.capacity_lbs))
        ]
    }


@app.post("/import/elevators")
async def import_elevators(
    json_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Import elevator data from uploaded JSON file (elevators.json format)
    """
    import json
    
    try:
        content = await json_file.read()
        data = json.loads(content.decode('utf-8'))
        
        # Clear existing elevator data
        db.query(ElevatorCostPerStop).delete()
        db.query(ElevatorCost).delete()
        db.query(ElevatorType).delete()
        db.commit()
        
        source_page = data.get('metadata', {}).get('pdfPage', 701)
        imported_types = 0
        imported_costs = 0
        imported_per_stops = 0
        
        # Process passenger and freight categories
        for category in ['passenger', 'freight']:
            if category not in data:
                continue
                
            for name, elevator_data in data[category].items():
                # Create elevator type
                elevator_type = ElevatorType(
                    category=category,
                    name=name,
                    source_page=source_page
                )
                db.add(elevator_type)
                db.flush()  # Get the ID
                imported_types += 1
                
                speeds = elevator_data.get('speeds', [])
                capacities = elevator_data.get('capacities', [])
                costs = elevator_data.get('costs', [])
                
                # Import cost matrix (speeds x capacities)
                for speed_idx, speed in enumerate(speeds):
                    if speed_idx < len(costs):
                        for cap_idx, capacity in enumerate(capacities):
                            if cap_idx < len(costs[speed_idx]):
                                cost_value = costs[speed_idx][cap_idx]
                                cost_entry = ElevatorCost(
                                    elevator_type_id=elevator_type.id,
                                    speed_fpm=speed,
                                    capacity_lbs=capacity,
                                    base_cost=cost_value
                                )
                                db.add(cost_entry)
                                imported_costs += 1
                
                # Import cost per stop (handle different key names)
                cost_per_stop_keys = [
                    ('cost_per_stop', 'standard'),
                    ('cost_per_stop_manual', 'manual'),
                    ('cost_per_stop_power', 'power'),
                ]
                
                for key, door_type in cost_per_stop_keys:
                    if key in elevator_data:
                        for cap_str, cost_value in elevator_data[key].items():
                            cps_entry = ElevatorCostPerStop(
                                elevator_type_id=elevator_type.id,
                                capacity_lbs=int(cap_str),
                                door_type=door_type,
                                cost_per_stop=cost_value
                            )
                            db.add(cps_entry)
                            imported_per_stops += 1
        
        db.commit()
        
        return {
            "success": True,
            "elevator_types": imported_types,
            "costs": imported_costs,
            "cost_per_stops": imported_per_stops
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ============ PDF VERSION ENDPOINTS ============

@app.get("/pdf-versions")
async def list_pdf_versions(db: Session = Depends(get_db)):
    """List all PDF versions"""
    versions = db.query(PdfVersion).order_by(PdfVersion.created_at.desc()).all()
    return {
        "count": len(versions),
        "versions": [
            {
                "id": v.id,
                "version_name": v.version_name,
                "edition_year": v.edition_year,
                "file_size_bytes": v.file_size_bytes,
                "is_active": v.is_active,
                "is_fully_parsed": v.is_fully_parsed,
                "created_at": v.created_at.isoformat() if v.created_at else None,
            }
            for v in versions
        ]
    }


@app.get("/pdf-versions/active")
async def get_active_pdf_version(db: Session = Depends(get_db)):
    """Get the currently active PDF version"""
    version = db.query(PdfVersion).filter(PdfVersion.is_active == True).first()
    if not version:
        raise HTTPException(status_code=404, detail="No active PDF version set")
    return {
        "id": version.id,
        "version_name": version.version_name,
        "edition_year": version.edition_year,
        "storage_path": version.storage_path,
        "is_fully_parsed": version.is_fully_parsed,
    }


@app.get("/pdf-versions/{version_id}")
async def get_pdf_version(version_id: int, db: Session = Depends(get_db)):
    """Get a specific PDF version"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    return {
        "id": version.id,
        "version_name": version.version_name,
        "edition_year": version.edition_year,
        "file_size_bytes": version.file_size_bytes,
        "file_hash": version.file_hash,
        "storage_path": version.storage_path,
        "original_filename": version.original_filename,
        "is_active": version.is_active,
        "is_fully_parsed": version.is_fully_parsed,
        "notes": version.notes,
        "created_at": version.created_at.isoformat() if version.created_at else None,
    }


@app.get("/pdf-versions/{version_id}/stats")
async def get_pdf_version_stats(version_id: int, db: Session = Depends(get_db)):
    """Get parsing statistics for a specific PDF version"""
    from app.database import StoryHeightMultiplier, FloorAreaPerimeterMultiplier
    
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    
    return {
        "version_id": version_id,
        "version_name": version.version_name,
        "local_multipliers": db.query(LocalMultiplier).filter(LocalMultiplier.pdf_version_id == version_id).count(),
        "current_cost_multipliers": db.query(CurrentCostMultiplier).filter(CurrentCostMultiplier.pdf_version_id == version_id).count(),
        "story_height_multipliers": db.query(StoryHeightMultiplier).filter(StoryHeightMultiplier.pdf_version_id == version_id).count(),
        "floor_area_perimeter_multipliers": db.query(FloorAreaPerimeterMultiplier).filter(FloorAreaPerimeterMultiplier.pdf_version_id == version_id).count(),
        "base_cost_tables": db.query(BaseCostTable).filter(BaseCostTable.pdf_version_id == version_id).count(),
        "elevator_types": db.query(ElevatorType).filter(ElevatorType.pdf_version_id == version_id).count(),
    }


# ============ ADMIN API ENDPOINTS ============

@app.get("/parsers")
async def list_parsers():
    """Return the registry of all known parsers"""
    return {"parsers": KNOWN_PARSERS}


@app.get("/pdf-versions/{version_id}/parse-runs")
async def list_parse_runs(version_id: int, db: Session = Depends(get_db)):
    """Get all parse run records for a specific PDF version, merged with the known parsers list"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    
    runs = db.query(ParseRun).filter(ParseRun.pdf_version_id == version_id).all()
    run_map = {r.parser_name: r for r in runs}
    
    import json as json_lib

    result = []
    for p in KNOWN_PARSERS:
        run = run_map.get(p["name"])
        # Parse diff_summary JSON string back to object for the response
        diff_data = None
        if run and run.diff_summary:
            try:
                diff_data = json_lib.loads(run.diff_summary)
            except (json_lib.JSONDecodeError, TypeError):
                diff_data = None

        result.append({
            "parser_name": p["name"],
            "label": p["label"],
            "description": p["description"],
            "section": p["section"],
            "page_type": p.get("page_type"),
            "default_start_page": p.get("default_start_page"),
            "default_end_page": p.get("default_end_page"),
            "status": run.status if run else "not_started",
            "records_created": run.records_created if run else 0,
            "error_message": run.error_message if run else None,
            "diff_summary": diff_data,
            "started_at": run.started_at.isoformat() if run and run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run and run.completed_at else None,
        })
    
    return {"version_id": version_id, "parse_runs": result}


@app.post("/pdf-versions/{version_id}/validate")
async def validate_pdf_version(version_id: int, db: Session = Depends(get_db)):
    """Run validation checks against the parsed data for a PDF version.
    Checks record counts against expected minimums for each parser."""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    
    # Expected minimum record counts per parser
    EXPECTED_MINIMUMS = {
        "local_multipliers": 800,
        "current_cost": 200,
        "story_height_s11": 10,
        "story_height_s13": 10,
        "story_height_s14": 10,
        "story_height_s15": 10,
        "floor_area_perimeter_s11": 100,
        "floor_area_perimeter_s13": 200,
        "floor_area_perimeter_s14": 200,
        "floor_area_perimeter_s15": 50,
        "base_cost_tables_s11": 30,
        "base_cost_tables_s13": 25,
        "base_cost_tables_s14": 35,
        "base_cost_tables_s15": 25,
        "sprinklers": 50,
        "hvac": 40,
        "elevators": 3,
    }
    
    runs = db.query(ParseRun).filter(ParseRun.pdf_version_id == version_id).all()
    run_map = {r.parser_name: r for r in runs}
    
    checks = []
    all_passed = True
    for p in KNOWN_PARSERS:
        run = run_map.get(p["name"])
        expected = EXPECTED_MINIMUMS.get(p["name"], 1)
        
        if not run:
            checks.append({"parser": p["name"], "label": p["label"], "passed": False, "reason": "Parser has not been run", "expected": expected, "actual": 0})
            all_passed = False
        elif run.status == "failed":
            checks.append({"parser": p["name"], "label": p["label"], "passed": False, "reason": f"Parser failed: {run.error_message}", "expected": expected, "actual": 0})
            all_passed = False
        elif run.status == "running":
            checks.append({"parser": p["name"], "label": p["label"], "passed": False, "reason": "Parser is still running", "expected": expected, "actual": run.records_created})
            all_passed = False
        elif run.records_created < expected:
            checks.append({"parser": p["name"], "label": p["label"], "passed": False, "reason": f"Record count {run.records_created} below expected minimum {expected}", "expected": expected, "actual": run.records_created})
            all_passed = False
        else:
            checks.append({"parser": p["name"], "label": p["label"], "passed": True, "reason": "OK", "expected": expected, "actual": run.records_created})
    
    return {"version_id": version_id, "all_passed": all_passed, "checks": checks}


@app.delete("/pdf-versions/{version_id}")
async def delete_pdf_version(version_id: int, db: Session = Depends(get_db)):
    """Delete a PDF version (cannot delete active version)"""
    version = db.query(PdfVersion).filter(PdfVersion.id == version_id).first()
    if not version:
        raise HTTPException(status_code=404, detail="PDF version not found")
    if version.is_active:
        raise HTTPException(status_code=400, detail="Cannot delete the active version")
    
    # Delete associated parse runs
    db.query(ParseRun).filter(ParseRun.pdf_version_id == version_id).delete()
    
    # Delete PDF file if it exists
    if version.storage_path and os.path.exists(version.storage_path):
        try:
            os.remove(version.storage_path)
        except OSError:
            pass
    
    db.delete(version)
    db.commit()
    return {"success": True, "deleted_version_id": version_id}


# ============ STATIC FRONTEND ============
# Serve the built frontend if it exists
frontend_dist = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")
if os.path.isdir(frontend_dist):
    from fastapi.responses import FileResponse

    # Serve static assets at /admin/assets/ to match Vite base: '/admin/'
    assets_dir = os.path.join(frontend_dist, "assets")
    if os.path.isdir(assets_dir):
        app.mount("/admin/assets", StaticFiles(directory=assets_dir), name="static-assets")

    @app.get("/admin")
    async def serve_frontend_root():
        return FileResponse(os.path.join(frontend_dist, "index.html"))

    @app.get("/admin/{full_path:path}")
    async def serve_frontend(full_path: str = ""):
        return FileResponse(os.path.join(frontend_dist, "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
