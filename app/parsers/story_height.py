"""
Story Height Multiplier Parser - Database Integration Wrapper
Uses the original MVS_Agent parser logic and writes to PostgreSQL.
Supports section-specific story height tables (S11, S13, S14, S15, etc.)
"""

from typing import List, Dict
from sqlalchemy.orm import Session
from app.database import StoryHeightMultiplier

# Import from the original parser
from app.parsers.story_height_original import parse_story_height_table


# Section-specific PDF page numbers for story height tables
SECTION_STORY_HEIGHT_PAGES = {
    11: 90,   # S11 P36 - base height 10 ft
    13: 218,  # S13 P42 - base height 12 ft
    14: 215,  # S14 P39 - base height 14 ft
    # 15: TBD - pages 38-40 not yet extracted from PDF
}


def parse_and_save(pdf_path: str, db: Session, page: int = 90, section: int = 11, pdf_version_id: int = None) -> int:
    """
    Parse story height multipliers from PDF using original parser and save to database.
    Only clears and replaces records for the specified section.
    
    Args:
        pdf_path: Path to MVS PDF file
        db: SQLAlchemy database session
        page: Page number (1-indexed), default 90 (S11)
        section: MVS section number (11, 13, 14, 15, etc.)
        pdf_version_id: Optional PDF version ID to associate with records
    
    Returns:
        Number of records updated
    """
    print(f"[StoryHeight] Parsing section {section} from: {pdf_path}")
    print(f"[StoryHeight] Page {page}")
    
    # Use the original parser
    results = parse_story_height_table(pdf_path, page)
    
    if not results['success']:
        raise Exception(f"Parser failed: {results['errors']}")
    
    multipliers = results['multipliers']
    print(f"[StoryHeight] Parsed {len(multipliers)} entries for section {section}")
    
    # Clear existing data for this section only
    query = db.query(StoryHeightMultiplier).filter(StoryHeightMultiplier.section == section)
    if pdf_version_id:
        query = query.filter(StoryHeightMultiplier.pdf_version_id == pdf_version_id)
    deleted = query.delete()
    db.commit()
    print(f"[StoryHeight] Cleared {deleted} existing records for section {section}")
    
    # Save to database
    for m in multipliers:
        record = StoryHeightMultiplier(
            section=section,
            height_meters=m.get('height_meters', 0),
            height_feet=m['height_feet'],
            sqft_multiplier=m['sqft_multiplier'],
            cuft_multiplier=m['cuft_multiplier'],
            source_page=m.get('source_page', page),
            pdf_version_id=pdf_version_id,
        )
        db.add(record)
    
    db.commit()
    print(f"[StoryHeight] Saved {len(multipliers)} records for section {section}")
    
    return len(multipliers)


def parse_all_sections(pdf_path: str, db: Session, pdf_version_id: int = None) -> Dict[int, int]:
    """
    Parse story height multipliers for all known sections.
    
    Returns:
        Dict mapping section number to record count
    """
    results = {}
    for section, page in SECTION_STORY_HEIGHT_PAGES.items():
        try:
            count = parse_and_save(pdf_path, db, page=page, section=section, pdf_version_id=pdf_version_id)
            results[section] = count
        except Exception as e:
            print(f"[StoryHeight] Failed to parse section {section}: {e}")
            results[section] = 0
    return results
