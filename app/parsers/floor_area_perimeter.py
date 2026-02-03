"""
Floor Area / Perimeter Multiplier Parser - Database Integration Wrapper
Uses the original MVS_Agent parser logic and writes to PostgreSQL
"""

from typing import List, Dict
from sqlalchemy.orm import Session
from app.database import FloorAreaPerimeterMultiplier

# Import from the original parser
from app.parsers.floor_area_perimeter_original import parse_area_perimeter_table


def parse_and_save(pdf_path: str, db: Session, page: int = 90) -> int:
    """
    Parse floor area/perimeter multipliers from PDF using original parser and save to database
    
    Args:
        pdf_path: Path to MVS PDF file
        db: SQLAlchemy database session
        page: Page number (1-indexed), default 90
    
    Returns:
        Number of records updated
    """
    print(f"[FloorAreaPerimeter] Parsing from: {pdf_path}")
    print(f"[FloorAreaPerimeter] Page {page}")
    
    # Use the original parser
    results = parse_area_perimeter_table(pdf_path, page)
    
    if not results['success']:
        raise Exception(f"Parser failed: {results['errors']}")
    
    multipliers = results['multipliers']
    print(f"[FloorAreaPerimeter] Parsed {len(multipliers)} entries")
    
    # Clear existing data
    db.query(FloorAreaPerimeterMultiplier).delete()
    db.commit()
    print("[FloorAreaPerimeter] Cleared existing records")
    
    # Save to database
    for m in multipliers:
        record = FloorAreaPerimeterMultiplier(
            floor_area_sqft=m['floor_area_sqft'],
            perimeter_ft=m['perimeter_ft'],
            multiplier=m['multiplier'],
            source_page=m.get('source_page', page)
        )
        db.add(record)
    
    db.commit()
    print(f"[FloorAreaPerimeter] Saved {len(multipliers)} records to database")
    
    return len(multipliers)
