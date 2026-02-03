from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import os

from app.database import get_db, init_db, BaseCostTable, BaseCostRow
from app.parsers import local_multipliers, current_cost, story_height, floor_area_perimeter
from app.parsers import base_cost_tables

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
        SessionLocal = get_session_local()
        db = SessionLocal()
        db.execute("ALTER TABLE mvs_base_cost_rows ALTER COLUMN building_class TYPE TEXT")
        db.execute("ALTER TABLE mvs_base_cost_rows ALTER COLUMN quality_type TYPE TEXT")
        db.commit()
        db.close()
        print("[MVS Parser Service] TEXT columns migration completed")
    except Exception as e:
        print(f"[MVS Parser Service] Migration skipped or failed: {e}")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "mvs-parser-service"}


@app.post("/migrate/fix-text-columns")
async def migrate_fix_text_columns(db: Session = Depends(get_db)):
    """Migrate VARCHAR columns to TEXT for base cost rows"""
    try:
        db.execute("ALTER TABLE mvs_base_cost_rows ALTER COLUMN building_class TYPE TEXT")
        db.execute("ALTER TABLE mvs_base_cost_rows ALTER COLUMN quality_type TYPE TEXT")
        db.commit()
        return {"success": True, "message": "Columns migrated to TEXT"}
    except Exception as e:
        db.rollback()
        return {"success": False, "error": str(e)}


@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Get current record counts from database"""
    from app.database import LocalMultiplier, CurrentCostMultiplier, StoryHeightMultiplier, FloorAreaPerimeterMultiplier
    
    return {
        "local_multipliers": db.query(LocalMultiplier).count(),
        "current_cost_multipliers": db.query(CurrentCostMultiplier).count(),
        "story_height_multipliers": db.query(StoryHeightMultiplier).count(),
        "floor_area_perimeter_multipliers": db.query(FloorAreaPerimeterMultiplier).count(),
        "base_cost_tables": db.query(BaseCostTable).count(),
        "base_cost_rows": db.query(BaseCostRow).count(),
    }


@app.post("/parse/local-multipliers")
async def parse_local_multipliers_endpoint(
    pdf_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Parse local multipliers from uploaded PDF and update database"""
    try:
        # Save uploaded file temporarily
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        result = local_multipliers.parse_and_save(temp_path, db)
        
        # Clean up
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse/current-cost")
async def parse_current_cost_endpoint(
    pdf_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Parse current cost multipliers from uploaded PDF and update database"""
    try:
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        result = current_cost.parse_and_save(temp_path, db)
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse/story-height")
async def parse_story_height_endpoint(
    pdf_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Parse story height multipliers from uploaded PDF and update database"""
    try:
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        result = story_height.parse_and_save(temp_path, db)
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parse/floor-area-perimeter")
async def parse_floor_area_perimeter_endpoint(
    pdf_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Parse floor area/perimeter multipliers from uploaded PDF and update database"""
    try:
        temp_path = f"/tmp/{pdf_file.filename}"
        with open(temp_path, "wb") as f:
            content = await pdf_file.read()
            f.write(content)
        
        result = floor_area_perimeter.parse_and_save(temp_path, db)
        os.remove(temp_path)
        
        return {"success": True, "records_updated": result}
    except Exception as e:
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
    db: Session = Depends(get_db)
):
    """
    Import base cost tables from uploaded markdown files
    
    Args:
        section: Section number (used for grouping and clearing existing)
        files: List of markdown files to import
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
        result = base_cost_tables.import_from_directory(temp_dir, db, section)
        
        # Clean up
        shutil.rmtree(temp_dir)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
