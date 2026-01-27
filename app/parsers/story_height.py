"""
Story Height Multiplier Parser - Database Integration Wrapper
Uses the original MVS_Agent parser logic and writes to PostgreSQL
"""

from typing import List, Dict
from sqlalchemy.orm import Session
from app.database import StoryHeightMultiplier

# Import from the original parser
from app.parsers.story_height_original import parse_height_multiplier_table


def parse_and_save(pdf_path: str, db: Session, page: int = 90) -> int:
    """
    Parse story height multipliers from PDF using original parser and save to database
    
    Args:
        pdf_path: Path to MVS PDF file
        db: SQLAlchemy database session
        page: Page number (1-indexed), default 90
    
    Returns:
        Number of records updated
    """
    print(f"[StoryHeight] Parsing from: {pdf_path}")
    print(f"[StoryHeight] Page {page}")
    
    # Use the original parser
    results = parse_height_multiplier_table(pdf_path, page, page)
    
    if not results['success']:
        raise Exception(f"Parser failed: {results['errors']}")
    
    multipliers = results['multipliers']
    print(f"[StoryHeight] Parsed {len(multipliers)} entries")
    
    # Clear existing data
    db.query(StoryHeightMultiplier).delete()
    db.commit()
    print("[StoryHeight] Cleared existing records")
    
    # Save to database
    for m in multipliers:
        record = StoryHeightMultiplier(
            height_meters=m.get('height_meters', 0),
            height_feet=m['height_feet'],
            sqft_multiplier=m['sqft_multiplier'],
            cuft_multiplier=m['cuft_multiplier'],
            source_page=m.get('source_page', page)
        )
        db.add(record)
    
    db.commit()
    print(f"[StoryHeight] Saved {len(multipliers)} records to database")
    
    return len(multipliers)
