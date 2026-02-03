"""
Base Cost Table Parser
Parses markdown files containing base cost tables and saves to PostgreSQL
"""

import re
import os
from pathlib import Path
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from app.database import BaseCostTable, BaseCostRow


def parse_markdown_file(file_path: str) -> Optional[Dict]:
    """
    Parse a single markdown file containing a base cost table
    
    Returns dict with:
        - name: Table name
        - occupancy_code: Code like "984"
        - section: Section number
        - page: Page number within section
        - pdf_page: Actual PDF page
        - notes: Notes section text
        - rows: List of cost row dicts
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"[BaseCostTables] Error reading {file_path}: {e}")
        return None
    
    result = {
        'file_name': os.path.basename(file_path),
        'name': None,
        'occupancy_code': None,
        'section': None,
        'page': None,
        'pdf_page': None,
        'notes': None,
        'rows': []
    }
    
    lines = content.split('\n')
    
    # Parse header - first line is title
    for line in lines:
        line = line.strip()
        if line.startswith('# '):
            # Extract name and occupancy code from title like "# LUXURY APARTMENTS (HIGH-RISE) (984)"
            title = line[2:].strip()
            # Try to extract occupancy code from parentheses at end
            code_match = re.search(r'\((\d+)\)\s*$', title)
            if code_match:
                result['occupancy_code'] = code_match.group(1)
                result['name'] = title[:code_match.start()].strip()
            else:
                result['name'] = title
            break
    
    # Parse metadata
    for line in lines:
        line = line.strip()
        
        # Occupancy Code
        if line.startswith('**Occupancy Code:**'):
            code = line.replace('**Occupancy Code:**', '').strip()
            if code:
                result['occupancy_code'] = code
        
        # Source (Section and Page)
        if line.startswith('**Source:**'):
            source = line.replace('**Source:**', '').strip()
            # Parse "Marshall Valuation Service, Section 11, Page 15"
            section_match = re.search(r'Section\s+(\d+)', source)
            page_match = re.search(r'Page\s+(\d+)', source)
            if section_match:
                result['section'] = int(section_match.group(1))
            if page_match:
                result['page'] = int(page_match.group(1))
        
        # PDF Page
        if line.startswith('**PDF Page:**'):
            pdf_page = line.replace('**PDF Page:**', '').strip()
            if pdf_page.isdigit():
                result['pdf_page'] = int(pdf_page)
    
    # Parse notes section
    notes_start = content.find('## Notes')
    if notes_start != -1:
        notes_section = content[notes_start:]
        # Skip the header line
        notes_lines = notes_section.split('\n')[1:]
        result['notes'] = '\n'.join(notes_lines).strip()
    
    # Parse cost table
    table_start = content.find('## Cost Table')
    if table_start != -1:
        table_section = content[table_start:]
        # Find the table rows (lines starting with |)
        table_lines = []
        for line in table_section.split('\n'):
            if line.strip().startswith('|'):
                table_lines.append(line.strip())
        
        if len(table_lines) >= 3:  # Header, separator, at least one row
            # Parse header to get column names
            header = table_lines[0]
            columns = [col.strip() for col in header.split('|')[1:-1]]
            
            # Skip separator line (index 1)
            # Parse data rows
            for row_idx, row_line in enumerate(table_lines[2:]):
                cells = [cell.strip() for cell in row_line.split('|')[1:-1]]
                
                if len(cells) >= len(columns):
                    row_data = parse_cost_row(columns, cells, row_idx)
                    if row_data:
                        result['rows'].append(row_data)
    
    # Only return if we have valid data
    if result['name'] and result['section'] is not None:
        return result
    
    return None


def parse_cost_row(columns: List[str], cells: List[str], row_order: int) -> Optional[Dict]:
    """Parse a single row from the cost table"""
    row = {
        'building_class': None,
        'quality_type': None,
        'exterior_walls': None,
        'interior_finish': None,
        'lighting_plumbing': None,
        'heat': None,
        'cost_sqm': None,
        'cost_cuft': None,
        'cost_sqft': None,
        'row_order': row_order
    }
    
    # Map columns to row fields
    column_mapping = {
        'CLASS': 'building_class',
        'TYPE': 'quality_type',
        'EXTERIOR WALLS': 'exterior_walls',
        'INTERIOR FINISH': 'interior_finish',
        'LIGHTING AND PLUMBING': 'lighting_plumbing',
        'LIGHTING, PLUMBING AND MECHANICAL': 'lighting_plumbing',
        'HEAT': 'heat',
        'Sq. M.': 'cost_sqm',
        'COST Cu. Ft.': 'cost_cuft',
        'Cu. Ft.': 'cost_cuft',
        'Sq. Ft.': 'cost_sqft',
    }
    
    for i, col_name in enumerate(columns):
        if i >= len(cells):
            break
        
        col_upper = col_name.upper().strip()
        cell_value = cells[i].strip()
        
        # Find matching field
        for key, field in column_mapping.items():
            if key.upper() in col_upper or col_upper in key.upper():
                if field in ['cost_sqm', 'cost_cuft', 'cost_sqft']:
                    # Parse numeric value
                    try:
                        # Remove commas and parse
                        num_val = float(cell_value.replace(',', ''))
                        row[field] = num_val
                    except ValueError:
                        pass
                else:
                    row[field] = cell_value if cell_value else None
                break
    
    # Must have at least building class
    if row['building_class']:
        return row
    
    return None


def parse_directory(directory_path: str) -> List[Dict]:
    """Parse all markdown files in a directory"""
    tables = []
    path = Path(directory_path)
    
    for md_file in path.glob('*.md'):
        result = parse_markdown_file(str(md_file))
        if result:
            tables.append(result)
            print(f"[BaseCostTables] Parsed: {md_file.name} ({len(result['rows'])} rows)")
    
    return tables


def save_tables_to_db(tables: List[Dict], db: Session, section: int = None) -> int:
    """
    Save parsed tables to database
    
    Args:
        tables: List of parsed table dicts
        db: Database session
        section: If specified, only delete tables from this section before inserting
    
    Returns:
        Number of tables saved
    """
    # Clear existing tables for the section if specified
    if section is not None:
        existing = db.query(BaseCostTable).filter(BaseCostTable.section == section).all()
        for table in existing:
            db.delete(table)
        db.commit()
        print(f"[BaseCostTables] Cleared existing tables for Section {section}")
    
    saved_count = 0
    
    for table_data in tables:
        # Create table record
        table = BaseCostTable(
            name=table_data['name'],
            occupancy_code=table_data.get('occupancy_code'),
            section=table_data['section'],
            page=table_data['page'],
            pdf_page=table_data.get('pdf_page'),
            notes=table_data.get('notes'),
            file_name=table_data.get('file_name')
        )
        db.add(table)
        db.flush()  # Get the table ID
        
        # Add rows
        for row_data in table_data['rows']:
            row = BaseCostRow(
                table_id=table.id,
                building_class=row_data['building_class'],
                quality_type=row_data.get('quality_type'),
                exterior_walls=row_data.get('exterior_walls'),
                interior_finish=row_data.get('interior_finish'),
                lighting_plumbing=row_data.get('lighting_plumbing'),
                heat=row_data.get('heat'),
                cost_sqm=row_data.get('cost_sqm'),
                cost_cuft=row_data.get('cost_cuft'),
                cost_sqft=row_data.get('cost_sqft'),
                row_order=row_data.get('row_order', 0)
            )
            db.add(row)
        
        saved_count += 1
    
    db.commit()
    print(f"[BaseCostTables] Saved {saved_count} tables to database")
    
    return saved_count


def import_from_directory(directory_path: str, db: Session, section: int = None) -> Dict:
    """
    Import all markdown files from a directory
    
    Args:
        directory_path: Path to directory containing .md files
        db: Database session
        section: Section number (used to clear existing and for metadata)
    
    Returns:
        Dict with import results
    """
    print(f"[BaseCostTables] Importing from: {directory_path}")
    
    tables = parse_directory(directory_path)
    
    if not tables:
        return {
            'success': False,
            'error': 'No valid tables found in directory',
            'tables_imported': 0,
            'total_rows': 0
        }
    
    # Override section if specified
    if section is not None:
        for table in tables:
            table['section'] = section
    
    saved = save_tables_to_db(tables, db, section)
    total_rows = sum(len(t['rows']) for t in tables)
    
    return {
        'success': True,
        'tables_imported': saved,
        'total_rows': total_rows
    }
