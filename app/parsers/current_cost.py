"""
Current Cost Multiplier Parser - Database Integration Wrapper
Uses the original MVS_Agent parser logic and writes to PostgreSQL
"""

from typing import List, Dict
from sqlalchemy.orm import Session
from app.database import CurrentCostMultiplier, RegionMapping

# Import the original parser functions
from app.parsers.current_cost_original import (
    parse_current_cost_multiplier_table,
    parse_page_current_cost,
    parse_region_based_multipliers,
    parse_single_table_text,
    parse_multiplier_table,
    get_region_for_state,
)


# State to region mapping
STATE_REGION_MAPPING = {
    'Eastern': [
        ('ME', 'MAINE'), ('NH', 'NEW HAMPSHIRE'), ('VT', 'VERMONT'),
        ('MA', 'MASSACHUSETTS'), ('RI', 'RHODE ISLAND'), ('CT', 'CONNECTICUT'),
        ('NY', 'NEW YORK'), ('NJ', 'NEW JERSEY'), ('PA', 'PENNSYLVANIA'),
        ('DE', 'DELAWARE'), ('MD', 'MARYLAND'), ('VA', 'VIRGINIA'),
        ('WV', 'WEST VIRGINIA'), ('NC', 'NORTH CAROLINA'), ('SC', 'SOUTH CAROLINA'),
        ('GA', 'GEORGIA'), ('FL', 'FLORIDA'), ('DC', 'DISTRICT OF COLUMBIA'),
    ],
    'Central': [
        ('ND', 'NORTH DAKOTA'), ('SD', 'SOUTH DAKOTA'), ('NE', 'NEBRASKA'),
        ('KS', 'KANSAS'), ('OK', 'OKLAHOMA'), ('TX', 'TEXAS'),
        ('MN', 'MINNESOTA'), ('IA', 'IOWA'), ('MO', 'MISSOURI'),
        ('AR', 'ARKANSAS'), ('LA', 'LOUISIANA'), ('WI', 'WISCONSIN'),
        ('IL', 'ILLINOIS'), ('MI', 'MICHIGAN'), ('IN', 'INDIANA'),
        ('OH', 'OHIO'), ('KY', 'KENTUCKY'), ('TN', 'TENNESSEE'),
        ('MS', 'MISSISSIPPI'), ('AL', 'ALABAMA'),
    ],
    'Western': [
        ('WA', 'WASHINGTON'), ('OR', 'OREGON'), ('CA', 'CALIFORNIA'),
        ('NV', 'NEVADA'), ('ID', 'IDAHO'), ('MT', 'MONTANA'),
        ('WY', 'WYOMING'), ('UT', 'UTAH'), ('CO', 'COLORADO'),
        ('AZ', 'ARIZONA'), ('NM', 'NEW MEXICO'), ('AK', 'ALASKA'),
        ('HI', 'HAWAII'),
    ],
}


def parse_and_save(pdf_path: str, db: Session, page: int = 717, pdf_version_id: int = None) -> int:
    """
    Parse current cost multipliers from PDF using original parser and save to database.
    Version-isolated: only touches rows with the given pdf_version_id.
    
    Args:
        pdf_path: Path to MVS PDF file
        db: SQLAlchemy database session
        page: Page number (1-indexed), default 717
        pdf_version_id: PDF version ID to scope all writes to
    
    Returns:
        Number of records updated
    """
    print(f"[CurrentCost] Parsing from: {pdf_path}")
    print(f"[CurrentCost] Page {page}, version_id={pdf_version_id}")
    
    # Use the original parser
    results = parse_current_cost_multiplier_table(pdf_path, page, page)
    
    if not results['success']:
        raise Exception(f"Parser failed: {results['errors']}")
    
    multipliers = results['multipliers']
    print(f"[CurrentCost] Parsed {len(multipliers)} entries")
    
    # Version-isolated: only delete rows for THIS version
    if pdf_version_id:
        deleted = db.query(CurrentCostMultiplier).filter(CurrentCostMultiplier.pdf_version_id == pdf_version_id).delete()
        print(f"[CurrentCost] Cleared {deleted} existing records for version {pdf_version_id}")
    
    # Region mappings are static/global - only save if they don't exist yet
    existing_mappings = db.query(RegionMapping).count()
    if existing_mappings == 0:
        save_region_mappings(db)
    
    # Save multipliers to database
    for m in multipliers:
        record = CurrentCostMultiplier(
            method=m['method'],
            region=m['region'],
            building_class=m['building_class'],
            effective_date=m['effective_date'],
            multiplier=m['multiplier'],
            source_page=m['source_page'],
            pdf_version_id=pdf_version_id,
        )
        db.add(record)
    
    db.commit()
    print(f"[CurrentCost] Saved {len(multipliers)} records for version {pdf_version_id}")
    
    return len(multipliers)


def save_region_mappings(db: Session):
    """Save state to region mappings"""
    count = 0
    for region, states in STATE_REGION_MAPPING.items():
        for code, name in states:
            record = RegionMapping(
                state_code=code,
                state_name=name,
                current_cost_region=region
            )
            db.add(record)
            count += 1
    
    db.commit()
    print(f"[CurrentCost] Saved {count} region mappings")


