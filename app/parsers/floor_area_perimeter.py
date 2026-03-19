"""
Floor Area / Perimeter Multiplier Parser - Database Integration Wrapper
Uses the original MVS_Agent parser logic and writes to PostgreSQL.
Supports section-specific floor area/perimeter tables (S11, S13, S14, S15, etc.)
"""

from typing import List, Dict
from sqlalchemy.orm import Session
from app.database import FloorAreaPerimeterMultiplier

# Import from the original parser
from app.parsers.floor_area_perimeter_original import parse_area_perimeter_table


# Section-specific PDF page numbers for floor area/perimeter tables
# Some sections have multiple pages (e.g., S14 has P38 for small buildings and P39 for large)
SECTION_FAP_PAGES = {
    11: [90],        # S11 P36
    13: [217],       # S13 P41
    14: [214, 215],  # S14 P38 (small buildings) + P39 (large buildings 300k+ sqft)
    # 15: TBD - pages 38-40 not yet extracted from PDF
}


def parse_and_save(pdf_path: str, db: Session, page: int = 90, section: int = 11, pdf_version_id: int = None) -> int:
    """
    Parse floor area/perimeter multipliers from PDF using original parser and save to database.
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
    print(f"[FloorAreaPerimeter] Parsing section {section} from: {pdf_path}")
    print(f"[FloorAreaPerimeter] Page {page}")
    
    # Use the original parser
    results = parse_area_perimeter_table(pdf_path, page)
    
    if not results['success']:
        raise Exception(f"Parser failed: {results['errors']}")
    
    multipliers = results['multipliers']
    print(f"[FloorAreaPerimeter] Parsed {len(multipliers)} entries for section {section}")
    
    # Version-isolated: only delete rows for THIS version + section
    if pdf_version_id:
        deleted = db.query(FloorAreaPerimeterMultiplier).filter(
            FloorAreaPerimeterMultiplier.section == section,
            FloorAreaPerimeterMultiplier.pdf_version_id == pdf_version_id
        ).delete()
        print(f"[FloorAreaPerimeter] Cleared {deleted} existing records for section {section}, version {pdf_version_id}")
    
    # Save to database
    for m in multipliers:
        record = FloorAreaPerimeterMultiplier(
            section=section,
            floor_area_sqft=m['floor_area_sqft'],
            perimeter_ft=m['perimeter_ft'],
            multiplier=m['multiplier'],
            source_page=m.get('source_page', page),
            pdf_version_id=pdf_version_id,
        )
        db.add(record)
    
    db.commit()
    print(f"[FloorAreaPerimeter] Saved {len(multipliers)} records for section {section}")
    
    return len(multipliers)


def parse_and_save_section(pdf_path: str, db: Session, section: int, pdf_version_id: int = None) -> int:
    """
    Parse all floor area/perimeter pages for a given section.
    Some sections have multiple FA/P tables (e.g., S14 has two).
    
    Returns:
        Total number of records inserted for this section
    """
    pages = SECTION_FAP_PAGES.get(section, [90])
    
    # Version-isolated: only delete rows for THIS version + section
    if pdf_version_id:
        deleted = db.query(FloorAreaPerimeterMultiplier).filter(
            FloorAreaPerimeterMultiplier.section == section,
            FloorAreaPerimeterMultiplier.pdf_version_id == pdf_version_id
        ).delete()
        print(f"[FloorAreaPerimeter] Cleared {deleted} existing records for section {section}, version {pdf_version_id}")
    
    total = 0
    for page in pages:
        try:
            results = parse_area_perimeter_table(pdf_path, page)
            if not results['success']:
                print(f"[FloorAreaPerimeter] Parser failed for section {section} page {page}: {results['errors']}")
                continue
            
            multipliers = results['multipliers']
            for m in multipliers:
                record = FloorAreaPerimeterMultiplier(
                    section=section,
                    floor_area_sqft=m['floor_area_sqft'],
                    perimeter_ft=m['perimeter_ft'],
                    multiplier=m['multiplier'],
                    source_page=m.get('source_page', page),
                    pdf_version_id=pdf_version_id,
                )
                db.add(record)
            total += len(multipliers)
            print(f"[FloorAreaPerimeter] Parsed {len(multipliers)} entries from section {section} page {page}")
        except Exception as e:
            print(f"[FloorAreaPerimeter] Error parsing section {section} page {page}: {e}")
    
    db.commit()
    print(f"[FloorAreaPerimeter] Total: {total} records for section {section}")
    return total


def parse_all_sections(pdf_path: str, db: Session, pdf_version_id: int = None) -> Dict[int, int]:
    """
    Parse floor area/perimeter multipliers for all known sections.
    
    Returns:
        Dict mapping section number to record count
    """
    results = {}
    for section in SECTION_FAP_PAGES:
        try:
            count = parse_and_save_section(pdf_path, db, section=section, pdf_version_id=pdf_version_id)
            results[section] = count
        except Exception as e:
            print(f"[FloorAreaPerimeter] Failed section {section}: {e}")
            results[section] = 0
    return results
