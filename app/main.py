from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import os

from app.database import get_db, init_db, BaseCostTable, BaseCostRow, LocalMultiplier, CurrentCostMultiplier, ElevatorType, ElevatorCost, ElevatorCostPerStop
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


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "mvs-parser-service"}


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
    """Get current record counts from database"""
    from app.database import LocalMultiplier, CurrentCostMultiplier, StoryHeightMultiplier, FloorAreaPerimeterMultiplier
    
    return {
        "local_multipliers": db.query(LocalMultiplier).count(),
        "current_cost_multipliers": db.query(CurrentCostMultiplier).count(),
        "story_height_multipliers": db.query(StoryHeightMultiplier).count(),
        "floor_area_perimeter_multipliers": db.query(FloorAreaPerimeterMultiplier).count(),
        "base_cost_tables": db.query(BaseCostTable).count(),
        "base_cost_rows": db.query(BaseCostRow).count(),
        "elevator_types": db.query(ElevatorType).count(),
        "elevator_costs": db.query(ElevatorCost).count(),
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
