"""
Local Multiplier Parser - Database Integration Wrapper
Uses the original MVS_Agent parser logic and writes to PostgreSQL
"""

from typing import List, Dict
from sqlalchemy.orm import Session
from app.database import LocalMultiplier

# Import the original parser functions
from app.parsers.local_multipliers_original import (
    parse_local_multiplier_table,
    parse_page_multipliers,
    parse_table_data,
    parse_position_based_text,
    clean_text_spacing,
    get_country_for_region,
)


def parse_and_save(pdf_path: str, db: Session, start_page: int = 719, end_page: int = 724) -> int:
    """
    Parse local multipliers from PDF using original parser and save to database
    
    Args:
        pdf_path: Path to MVS PDF file
        db: SQLAlchemy database session
        start_page: Starting page (1-indexed), default 719
        end_page: Ending page (1-indexed), default 724
    
    Returns:
        Number of records updated
    """
    print(f"[LocalMultipliers] Parsing from: {pdf_path}")
    print(f"[LocalMultipliers] Pages {start_page} to {end_page}")
    
    # Use the original parser
    results = parse_local_multiplier_table(pdf_path, start_page, end_page)
    
    if not results['success']:
        raise Exception(f"Parser failed: {results['errors']}")
    
    multipliers = results['multipliers']
    print(f"[LocalMultipliers] Parsed {len(multipliers)} entries")
    
    # Clear existing data
    db.query(LocalMultiplier).delete()
    db.commit()
    print("[LocalMultipliers] Cleared existing records")
    
    # Save to database
    for m in multipliers:
        record = LocalMultiplier(
            location=m['location'],
            city=m.get('city'),
            region=m['region'],
            country=m['country'],
            class_a=m['class_a'],
            class_b=m['class_b'],
            class_c=m['class_c'],
            class_d=m['class_d'],
            class_s=m['class_s'],
            source_page=m['source_page'],
            is_regional=m.get('is_regional', False)
        )
        db.add(record)
    
    db.commit()
    print(f"[LocalMultipliers] Saved {len(multipliers)} records to database")
    
    return len(multipliers)
