"""
Sprinkler System Refinement Parser for Marshall Valuation Service
Extracts sprinkler system cost tables from MVS PDF

Table format:
- COVERAGE (Square Feet) as rows: 1,500 to 500,000
- WET SYSTEMS: LOW, AVG., GOOD, EXCL. columns
- DRY SYSTEMS: LOW, AVG., GOOD, EXCL. columns
"""

import pdfplumber
import json
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple


def extract_section_info(page_text: str) -> Tuple[str, str]:
    """Extract section number and page number from page header"""
    # Look for "SECTION XX PAGE YY" pattern
    match = re.search(r'SECTION\s+(\d+)\s+PAGE\s+(\d+)', page_text, re.IGNORECASE)
    if match:
        return match.group(1), match.group(2)
    return "", ""


def parse_sprinkler_table(pdf_path: str, pdf_page: int) -> Dict:
    """
    Parse sprinkler system cost table from a specific PDF page
    
    Args:
        pdf_path: Path to MVS PDF file
        pdf_page: PDF page number (1-indexed)
    
    Returns:
        Dictionary containing parsed sprinkler cost data and metadata
    """
    results = {
        'success': False,
        'section': '',
        'section_page': '',
        'pdf_page': pdf_page,
        'wet_systems': [],
        'dry_systems': [],
        'notes': '',
        'errors': []
    }
    
    try:
        print(f"[*] Opening PDF: {pdf_path}")
        print(f"[*] Processing PDF page {pdf_page}")
        
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            
            if pdf_page < 1 or pdf_page > total_pages:
                raise ValueError(f"Invalid page. PDF has {total_pages} pages.")
            
            page = pdf.pages[pdf_page - 1]
            page_text = page.extract_text() or ""
            
            # Extract section info
            section_num, section_page = extract_section_info(page_text)
            results['section'] = section_num
            results['section_page'] = section_page
            
            print(f"   Section {section_num}, Page {section_page}")
            
            # Check if this page has sprinkler data
            if 'SPRINKLERS' not in page_text.upper():
                print(f"   [!] No SPRINKLERS section found on this page")
                results['errors'].append("No SPRINKLERS section found")
                return results
            
            # Parse the sprinkler table
            wet_data, dry_data, notes = parse_sprinkler_data(page, page_text)
            
            results['wet_systems'] = wet_data
            results['dry_systems'] = dry_data
            results['notes'] = notes
            results['success'] = True
            
            print(f"   [+] Found {len(wet_data)} wet system entries")
            print(f"   [+] Found {len(dry_data)} dry system entries")
            
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    
    return results


def parse_sprinkler_data(page, page_text: str) -> Tuple[List[Dict], List[Dict], str]:
    """
    Parse sprinkler cost data from page
    
    Returns:
        Tuple of (wet_systems_data, dry_systems_data, notes_text)
    """
    wet_data = []
    dry_data = []
    notes = ""
    
    # Extract notes from the SPRINKLERS section using word positions
    # This avoids interleaving with ELEVATORS section on left side of page
    words = page.extract_words(x_tolerance=3, y_tolerance=3)
    
    # Find SPRINKLERS header position
    sprinkler_header_y = None
    sprinkler_x = None
    for word in words:
        if word['text'].upper() == 'SPRINKLERS':
            sprinkler_header_y = word['top']
            sprinkler_x = word['x0']
            break
    
    # Find COVERAGE header position (marks end of notes)
    coverage_y = None
    for word in words:
        if word['text'].upper() == 'COVERAGE':
            coverage_y = word['top']
            break
    
    if sprinkler_header_y and coverage_y and sprinkler_x:
        # Get words between SPRINKLERS and COVERAGE that are on the right side
        note_words = []
        for word in words:
            if (word['top'] > sprinkler_header_y and 
                word['top'] < coverage_y and 
                word['x0'] >= sprinkler_x - 150):  # Right side of page
                note_words.append(word)
        
        # Sort by position and join
        note_words.sort(key=lambda w: (w['top'], w['x0']))
        notes = ' '.join(w['text'] for w in note_words)
        notes = ' '.join(notes.split())  # Clean whitespace
    
    # Define expected coverage values
    coverage_values = [
        1500, 3000, 5000, 10000, 15000, 20000, 30000, 40000, 50000,
        75000, 100000, 125000, 150000, 200000, 300000, 400000, 500000
    ]
    
    # Reuse words already extracted above
    
    # Find the COVERAGE column position to identify sprinkler table start
    coverage_x = None
    for word in words:
        if word['text'].upper() == 'COVERAGE':
            coverage_x = word['x0']
            break
    
    if not coverage_x:
        print("   [!] Could not locate COVERAGE column header")
        return wet_data, dry_data, notes
    
    print(f"   Found COVERAGE column at x={coverage_x:.0f}")
    
    # Group words by approximate Y position to find rows
    from collections import defaultdict
    rows_by_y = defaultdict(list)
    
    for word in words:
        # Only look at words in the sprinkler section (starting near COVERAGE column)
        if word['x0'] >= coverage_x - 20:
            y_key = round(word['top'] / 8) * 8  # Group by ~8 pixel rows
            rows_by_y[y_key].append(word)
    
    # Sort rows by Y position
    sorted_rows = sorted(rows_by_y.items(), key=lambda x: x[0])
    
    # Find the data rows (rows that contain exactly 9 numbers with first being coverage)
    for y_pos, row_words in sorted_rows:
        # Sort words by X position
        row_words = sorted(row_words, key=lambda w: w['x0'])
        
        # Extract all numbers from this row
        numbers = []
        for word in row_words:
            text = word['text'].replace(',', '')
            try:
                num = float(text)
                numbers.append(num)
            except ValueError:
                continue
        
        # Check if we have 9 numbers (coverage + 4 wet + 4 dry)
        if len(numbers) == 9:
            coverage = int(numbers[0])
            if coverage in coverage_values or (1000 <= coverage <= 600000):
                wet_entry = {
                    'coverage_sqft': coverage,
                    'low': numbers[1],
                    'average': numbers[2],
                    'good': numbers[3],
                    'excellent': numbers[4]
                }
                dry_entry = {
                    'coverage_sqft': coverage,
                    'low': numbers[5],
                    'average': numbers[6],
                    'good': numbers[7],
                    'excellent': numbers[8]
                }
                wet_data.append(wet_entry)
                dry_data.append(dry_entry)
    
    return wet_data, dry_data, notes


def save_to_json(results: Dict, output_path: str) -> None:
    """
    Save parsed sprinkler costs to JSON file
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'source': f"Marshall Valuation Service - Section {results['section']}, Page {results['section_page']}",
                'pdf_page': results['pdf_page'],
                'description': 'Sprinkler system installation costs per square foot',
                'notes': results['notes']
            },
            'wet_systems': results['wet_systems'],
            'dry_systems': results['dry_systems']
        }, f, indent=2)
    
    print(f"[SAVED] JSON: {output_file}")


def save_to_markdown(results: Dict, output_path: str) -> None:
    """
    Save parsed sprinkler costs to markdown file with proper table formatting
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    section = results['section']
    section_page = results['section_page']
    pdf_page = results['pdf_page']
    wet_data = results['wet_systems']
    dry_data = results['dry_systems']
    notes = results['notes']
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# SPRINKLER SYSTEM COSTS - Section {section}\n\n")
        f.write(f"**Source:** Marshall Valuation Service, Section {section}, Page {section_page}\n")
        f.write(f"**PDF Page:** {pdf_page}\n\n")
        
        # Write notes if available
        if notes:
            f.write("## Notes\n\n")
            f.write(f"{notes}\n\n")
        
        # Write wet systems table
        if wet_data:
            f.write("## Wet Systems\n\n")
            f.write("*Cost per square foot based on coverage area*\n\n")
            f.write("| Coverage (Sq. Ft.) | Low | Average | Good | Excellent |\n")
            f.write("| --- | --- | --- | --- | --- |\n")
            
            for entry in wet_data:
                coverage = f"{entry['coverage_sqft']:,}"
                f.write(f"| {coverage} | {entry['low']:.2f} | {entry['average']:.2f} | {entry['good']:.2f} | {entry['excellent']:.2f} |\n")
            
            f.write("\n")
        
        # Write dry systems table
        if dry_data:
            f.write("## Dry Systems\n\n")
            f.write("*Cost per square foot based on coverage area*\n\n")
            f.write("| Coverage (Sq. Ft.) | Low | Average | Good | Excellent |\n")
            f.write("| --- | --- | --- | --- | --- |\n")
            
            for entry in dry_data:
                coverage = f"{entry['coverage_sqft']:,}"
                f.write(f"| {coverage} | {entry['low']:.2f} | {entry['average']:.2f} | {entry['good']:.2f} | {entry['excellent']:.2f} |\n")
    
    print(f"[SAVED] Markdown: {output_file}")


def main():
    """
    Main entry point for command-line usage
    
    Usage: python parse-sprinklers-refinement.py <pdf_path> <pdf_page>
    Example: python parse-sprinklers-refinement.py data/pdfs/MVS.pdf 89
    """
    if len(sys.argv) < 3:
        print("Usage: python parse-sprinklers-refinement.py <pdf_path> <pdf_page>")
        print("Example: python parse-sprinklers-refinement.py data/pdfs/MVS.pdf 89")
        print("\nExtracts sprinkler cost table from the specified PDF page.")
        print("Output files are named by section (e.g., S11_SPRINKLERS.md)")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    pdf_page = int(sys.argv[2])
    
    # Parse the PDF page
    results = parse_sprinkler_table(pdf_path, pdf_page)
    
    if results['success'] and results['section']:
        # Save output with section-based naming
        output_dir = Path(__file__).parent.parent / "Tables" / "Refinements" / "Sprinklers"
        section = results['section']
        
        # Save as markdown
        markdown_path = output_dir / f"S{section}_SPRINKLERS.md"
        save_to_markdown(results, str(markdown_path))
        
        # Save as JSON
        json_path = output_dir / f"S{section}_sprinklers.json"
        save_to_json(results, str(json_path))
        
        print(f"\n[SUCCESS] Section {section} sprinkler data extracted!")
        print(f"   Wet system entries: {len(results['wet_systems'])}")
        print(f"   Dry system entries: {len(results['dry_systems'])}")
    else:
        print("\n[ERROR] Processing failed!")
        for error in results['errors']:
            print(f"   - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
