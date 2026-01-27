from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
import os

from app.database import get_db, init_db
from app.parsers import local_multipliers, current_cost, story_height, floor_area_perimeter

app = FastAPI(
    title="MVS Parser Service",
    description="Service for parsing Marshall Valuation Service PDF data and writing to PostgreSQL",
    version="1.0.0"
)


@app.on_event("startup")
async def startup():
    # Ensure tables exist
    init_db()
    print("[MVS Parser Service] Database initialized")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "mvs-parser-service"}


@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Get current record counts from database"""
    from app.database import LocalMultiplier, CurrentCostMultiplier, StoryHeightMultiplier, FloorAreaPerimeterMultiplier
    
    return {
        "local_multipliers": db.query(LocalMultiplier).count(),
        "current_cost_multipliers": db.query(CurrentCostMultiplier).count(),
        "story_height_multipliers": db.query(StoryHeightMultiplier).count(),
        "floor_area_perimeter_multipliers": db.query(FloorAreaPerimeterMultiplier).count(),
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
