"""
Floor Area/Perimeter Multiplier Parser for Marshall Valuation Service
Extracts floor area/perimeter multiplier tables from MVS PDF

Each section has its own table (e.g., Section 11 on page 90).
Table structure:
- Rows: Floor Area (Sq Ft) from 1,500 to 40,000
- Columns: Average Perimeter (Ft) from 160 to 2,000  
- Values are multipliers (some cells are "----" for no data)
"""

import pdfplumber
import json
import re
import sys
from pathlib import Path
from typing import List, Dict, Tuple
from collections import defaultdict


def extract_section_info(page_text: str) -> Tuple[str, str]:
    """Extract section number and page from header text"""
    section = ""
    section_page = ""
    
    match = re.search(r'SECTION\s+(\d+)\s+PAGE\s+(\d+)', page_text, re.IGNORECASE)
    if match:
        section = match.group(1)
        section_page = match.group(2)
    
    return section, section_page


def parse_area_perimeter_table(pdf_path: str, pdf_page: int) -> Dict:
    """
    Parse floor area/perimeter multiplier table from specified PDF page
    
    Args:
        pdf_path: Path to MVS PDF file
        pdf_page: PDF page number (1-indexed)
    
    Returns:
        Dictionary containing parsed multiplier data and metadata
    """
    results = {
        'success': False,
        'section': '',
        'section_page': '',
        'pdf_page': pdf_page,
        'perimeter_values': [],
        'floor_area_values': [],
        'multipliers': [],
        'errors': []
    }
    
    try:
        print(f"[*] Opening PDF: {pdf_path}")
        print(f"[*] Processing PDF page {pdf_page}")
        
        with pdfplumber.open(pdf_path) as pdf:
            if pdf_page < 1 or pdf_page > len(pdf.pages):
                raise ValueError(f"Invalid page. PDF has {len(pdf.pages)} pages.")
            
            page = pdf.pages[pdf_page - 1]
            page_text = page.extract_text()
            
            # Extract section info
            section, section_page = extract_section_info(page_text)
            results['section'] = section
            results['section_page'] = section_page
            print(f"   Section {section}, Page {section_page}")
            
            # Parse the table data
            data = parse_floor_area_perimeter_data(page, page_text)
            
            if data:
                results['success'] = True
                results['perimeter_values'] = data['perimeter_values']
                results['floor_area_values'] = data['floor_area_values']
                results['multipliers'] = data['multipliers']
                print(f"   [+] Found {len(data['floor_area_values'])} floor areas x {len(data['perimeter_values'])} perimeters")
                print(f"   [+] Total multiplier entries: {len(data['multipliers'])}")
            else:
                results['errors'].append("No table data found")
                print(f"   [!] No table data found")
                
    except FileNotFoundError:
        error_msg = f"PDF file not found: {pdf_path}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    
    return results


def parse_floor_area_perimeter_data(page, page_text: str) -> Dict:
    """
    Parse floor area/perimeter multiplier data from page
    
    Returns dict with perimeter_values, floor_area_values, and multipliers list
    """
    # Extract words with positions
    words = page.extract_words(x_tolerance=3, y_tolerance=3)
    
    # Find boundaries: FLOOR AREA/PERIMETER MULTIPLIERS to STORY HEIGHT MULTIPLIERS
    table_start_y = None
    table_end_y = None
    
    for word in words:
        text = word['text'].upper()
        if 'PERIMETER' in text:
            nearby = [w for w in words if abs(w['top'] - word['top']) < 15]
            nearby_text = ' '.join(w['text'].upper() for w in nearby)
            if 'FLOOR' in nearby_text and 'MULTIPLIER' in nearby_text:
                table_start_y = word['top']
                print(f"   Found FLOOR AREA/PERIMETER MULTIPLIERS at y={table_start_y:.0f}")
        
        if text == 'STORY':
            nearby = [w for w in words if abs(w['top'] - word['top']) < 10]
            nearby_text = ' '.join(w['text'].upper() for w in nearby)
            if 'HEIGHT' in nearby_text and 'MULTIPLIER' in nearby_text:
                table_end_y = word['top']
                print(f"   Found STORY HEIGHT MULTIPLIERS at y={table_end_y:.0f}")
                break
    
    if not table_start_y:
        print("   [!] Could not locate FLOOR AREA/PERIMETER header")
        return None
    
    # Filter words to table region
    table_words = []
    for word in words:
        if table_start_y and word['top'] > table_start_y + 20:
            if table_end_y and word['top'] >= table_end_y - 10:
                continue
            table_words.append(word)
    
    # Group words by Y position (rows)
    # First pass: collect words with their raw Y positions
    rows_by_y = defaultdict(list)
    for word in table_words:
        y_key = round(word['top'])
        rows_by_y[y_key].append(word)
    
    # Second pass: merge rows that are within 2px of each other
    # This handles the case where floor area labels and multipliers have slight Y offsets
    merged_rows = {}
    sorted_y_keys = sorted(rows_by_y.keys())
    
    for y_key in sorted_y_keys:
        # Find if there's an existing merged row within 3px
        merged_to = None
        for existing_y in merged_rows.keys():
            if abs(y_key - existing_y) <= 3:
                merged_to = existing_y
                break
        
        if merged_to is not None:
            merged_rows[merged_to].extend(rows_by_y[y_key])
        else:
            merged_rows[y_key] = list(rows_by_y[y_key])
    
    sorted_rows = sorted(merged_rows.items(), key=lambda x: x[0])
    
    # Known perimeter values (ft) from the table header
    perimeter_values_ft = [160, 180, 200, 250, 300, 350, 400, 500, 600, 700, 800, 1000, 1200, 1400, 1600, 2000]
    
    # Known floor area values (sq ft) from the table
    floor_area_values_ft = [1500, 2000, 2500, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 
                           10000, 12000, 14000, 16000, 18000, 20000, 24000, 28000, 
                           32000, 36000, 40000]
    
    # First, find the X positions of the perimeter column headers
    # Look for the header row containing perimeter values
    perimeter_col_positions = {}
    for y_pos, row_words in sorted_rows[:5]:  # Check first few rows for header
        row_words_sorted = sorted(row_words, key=lambda w: w['x0'])
        for word in row_words_sorted:
            text = word['text'].replace(',', '').strip()
            try:
                num = int(float(text))
                if num in perimeter_values_ft:
                    perimeter_col_positions[num] = word['x0']
            except ValueError:
                continue
    
    if perimeter_col_positions:
        print(f"   Found {len(perimeter_col_positions)} perimeter column positions")
    
    multipliers = []
    
    # Parse each row looking for floor area and multiplier values
    for y_pos, row_words in sorted_rows:
        row_words = sorted(row_words, key=lambda w: w['x0'])
        
        # Find floor area value (should be in left part of row, within first ~80px)
        # The table has Sq. M. column first, then Sq. Ft. column
        floor_area_sqft = None
        floor_area_x = None
        
        for word in row_words[:5]:  # Check only first 5 words in row
            text = word['text'].replace(',', '').strip()
            try:
                num = int(float(text))
                if num in floor_area_values_ft:
                    floor_area_sqft = num
                    floor_area_x = word['x0']
                    break
            except ValueError:
                continue
        
        if not floor_area_sqft:
            continue
        
        # Extract multiplier values based on X position
        for word in row_words:
            text = word['text'].replace(',', '').strip()
            if text in ['---', '----', '--', '']:
                continue
            try:
                num = float(text)
                # Multipliers are between 0.8 and 1.5
                if 0.8 <= num <= 1.5:
                    # Find which perimeter column this belongs to based on X position
                    word_x = word['x0']
                    
                    # Match to nearest perimeter column
                    best_perimeter = None
                    best_dist = float('inf')
                    
                    for pval, px in perimeter_col_positions.items():
                        dist = abs(word_x - px)
                        if dist < best_dist and dist < 40:  # Within 40 pixels
                            best_dist = dist
                            best_perimeter = pval
                    
                    if best_perimeter:
                        multipliers.append({
                            'floor_area_sqft': floor_area_sqft,
                            'perimeter_ft': best_perimeter,
                            'multiplier': round(num, 3)
                        })
            except ValueError:
                continue
    
    # Remove duplicates (same floor_area + perimeter combination)
    seen = set()
    unique_multipliers = []
    for m in multipliers:
        key = (m['floor_area_sqft'], m['perimeter_ft'])
        if key not in seen:
            seen.add(key)
            unique_multipliers.append(m)
    
    if not unique_multipliers:
        return None
    
    return {
        'perimeter_values': perimeter_values_ft,
        'floor_area_values': floor_area_values_ft,
        'multipliers': unique_multipliers
    }


def save_to_json(results: Dict, output_path: str) -> None:
    """Save parsed multipliers to JSON file"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'source': f"Marshall Valuation Service - Section {results['section']}, Page {results['section_page']}",
                'pdf_page': results['pdf_page'],
                'description': 'Floor area/perimeter multipliers - adjustments based on building size and shape',
            },
            'perimeter_values_ft': results['perimeter_values'],
            'floor_area_values_sqft': results['floor_area_values'],
            'multipliers': results['multipliers']
        }, f, indent=2)
    
    print(f"[SAVED] JSON: {output_file}")


def save_to_markdown(results: Dict, output_path: str) -> None:
    """Save parsed multipliers to markdown file"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    section = results['section']
    section_page = results['section_page']
    pdf_page = results['pdf_page']
    multipliers = results['multipliers']
    perimeter_values = results['perimeter_values']
    floor_area_values = results['floor_area_values']
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# FLOOR AREA/PERIMETER MULTIPLIERS - Section {section}\n\n")
        f.write(f"**Source:** Marshall Valuation Service, Section {section}, Page {section_page}\n")
        f.write(f"**PDF Page:** {pdf_page}\n\n")
        f.write("*Multipliers adjust base cost based on floor area and perimeter dimensions*\n\n")
        
        if multipliers:
            # Create a matrix view grouped by floor area
            f.write("## Multiplier Table\n\n")
            
            # Header row with perimeter values
            header = "| Floor Area (SF) |"
            for p in perimeter_values:
                header += f" {p} |"
            f.write(header + "\n")
            
            # Separator row
            sep = "| --- |"
            for _ in perimeter_values:
                sep += " --- |"
            f.write(sep + "\n")
            
            # Data rows grouped by floor area
            for area in floor_area_values:
                row = f"| {area:,} |"
                area_mults = {m['perimeter_ft']: m['multiplier'] for m in multipliers if m['floor_area_sqft'] == area}
                for p in perimeter_values:
                    if p in area_mults:
                        row += f" {area_mults[p]:.3f} |"
                    else:
                        row += " --- |"
                f.write(row + "\n")
        else:
            f.write("*No multipliers extracted*\n")
    
    print(f"[SAVED] Markdown: {output_file}")


def main():
    """
    Main entry point for command-line usage
    
    Usage: python parse-area-perimeter-multiplier.py <pdf_path> <pdf_page>
    Example: python parse-area-perimeter-multiplier.py data/pdfs/MVS.pdf 90
    """
    if len(sys.argv) < 3:
        print("Usage: python parse-area-perimeter-multiplier.py <pdf_path> <pdf_page>")
        print("Example: python parse-area-perimeter-multiplier.py data/pdfs/MVS.pdf 90")
        print("\nExtracts floor area/perimeter multiplier table from the specified PDF page.")
        print("Output files are named by section (e.g., S11_FLOOR_AREA_PERIMETER.md)")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    pdf_page = int(sys.argv[2])
    
    # Parse the PDF page
    results = parse_area_perimeter_table(pdf_path, pdf_page)
    
    if results['success'] and results['section']:
        # Save output with section-based naming
        output_dir = Path(__file__).parent.parent / "Tables" / "Refinements" / "FloorAreaPerimeter"
        section = results['section']
        
        # Save as markdown
        markdown_path = output_dir / f"S{section}_FLOOR_AREA_PERIMETER.md"
        save_to_markdown(results, str(markdown_path))
        
        # Save as JSON
        json_path = output_dir / f"S{section}_floor_area_perimeter.json"
        save_to_json(results, str(json_path))
        
        print(f"\n[SUCCESS] Section {section} floor area/perimeter multipliers extracted!")
        print(f"   Total entries: {len(results['multipliers'])}")
    else:
        print("\n[ERROR] Processing failed!")
        for error in results['errors']:
            print(f"   - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
