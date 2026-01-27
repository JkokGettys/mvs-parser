"""
Story Height Multiplier Parser for Marshall Valuation Service
Extracts story height multiplier tables from MVS PDF

Each section has its own story height table (e.g., Section 11 on page 90).
The table shows multipliers for varying wall heights (7-24 ft).
Base height is 10 ft (3.05m) with multiplier 1.000.
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
    
    # Look for "SECTION XX PAGE YY" pattern
    match = re.search(r'SECTION\s+(\d+)\s+PAGE\s+(\d+)', page_text, re.IGNORECASE)
    if match:
        section = match.group(1)
        section_page = match.group(2)
    
    return section, section_page


def parse_story_height_table(pdf_path: str, pdf_page: int) -> Dict:
    """
    Parse story height multiplier table from specified PDF page
    
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
            
            # Parse the story height data
            multipliers = parse_story_height_data(page, page_text)
            
            if multipliers:
                results['success'] = True
                results['multipliers'] = multipliers
                print(f"   [+] Found {len(multipliers)} height multiplier entries")
            else:
                results['errors'].append("No multipliers found on page")
                print(f"   [!] No multipliers found")
                
    except FileNotFoundError:
        error_msg = f"PDF file not found: {pdf_path}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    
    return results


def parse_story_height_data(page, page_text: str) -> List[Dict]:
    """
    Parse story height multiplier data from page
    
    The table has two columns (left and right) with:
    - Average Wall Height (M.)
    - Average Wall Height (FT.)
    - Square Foot/Meter Multiplier
    - Cubic Foot Multiplier
    """
    multipliers = []
    
    # Extract words with positions
    words = page.extract_words(x_tolerance=3, y_tolerance=3)
    
    # Find "STORY HEIGHT MULTIPLIERS" header to identify table start
    story_height_y = None
    for word in words:
        if 'STORY' in word['text'].upper() and 'HEIGHT' in page_text.upper():
            # Check if this is part of STORY HEIGHT MULTIPLIERS
            story_height_y = word['top']
            break
    
    # Better approach: find the Y position where "STORY HEIGHT MULTIPLIERS" appears
    for i, word in enumerate(words):
        if word['text'].upper() == 'STORY':
            # Look for HEIGHT and MULTIPLIERS nearby
            nearby = [w for w in words if abs(w['top'] - word['top']) < 10]
            nearby_text = ' '.join(w['text'].upper() for w in nearby)
            if 'HEIGHT' in nearby_text and 'MULTIPLIER' in nearby_text:
                story_height_y = word['top']
                print(f"   Found STORY HEIGHT MULTIPLIERS at y={story_height_y:.0f}")
                break
    
    if not story_height_y:
        print("   [!] Could not locate STORY HEIGHT MULTIPLIERS header")
        return multipliers
    
    # Group words by Y position (rows) - only words below the header
    rows_by_y = defaultdict(list)
    for word in words:
        if word['top'] > story_height_y + 30:  # Below the header
            y_key = round(word['top'] / 10) * 10
            rows_by_y[y_key].append(word)
    
    # Sort rows by Y
    sorted_rows = sorted(rows_by_y.items(), key=lambda x: x[0])
    
    # Find data rows - they should have numeric values
    # Expected pattern: meters, feet, sqft_mult, cuft_mult (repeated twice for left/right columns)
    for y_pos, row_words in sorted_rows:
        row_words = sorted(row_words, key=lambda w: w['x0'])
        
        # Extract numbers from this row
        numbers = []
        for word in row_words:
            text = word['text'].replace(',', '').replace('(base)', '')
            try:
                num = float(text)
                numbers.append((num, word['x0']))
            except ValueError:
                continue
        
        # We need at least 4 numbers for one column, or 8 for both
        if len(numbers) >= 4:
            # Determine if this is a data row by checking if values are reasonable
            # Wall height in meters: 2-8, feet: 7-24, multipliers: 0.5-2.0
            
            # Split into left and right column based on X position
            page_mid = 400  # Approximate middle of page
            
            left_nums = [n for n, x in numbers if x < page_mid]
            right_nums = [n for n, x in numbers if x >= page_mid]
            
            # Process left column if we have 4 numbers
            if len(left_nums) >= 4:
                entry = create_entry(left_nums[:4])
                if entry:
                    multipliers.append(entry)
            
            # Process right column if we have 4 numbers
            if len(right_nums) >= 4:
                entry = create_entry(right_nums[:4])
                if entry:
                    multipliers.append(entry)
    
    # Sort by feet value
    multipliers.sort(key=lambda x: x['height_feet'])
    
    return multipliers


def create_entry(nums: List[float]) -> Dict:
    """
    Create a multiplier entry from 4 numbers: meters, feet, sqft_mult, cuft_mult
    """
    if len(nums) < 4:
        return None
    
    meters, feet, sqft_mult, cuft_mult = nums[0], nums[1], nums[2], nums[3]
    
    # Validate reasonable values
    # Meters: 2-8, Feet: 7-25, Multipliers: 0.5-2.0
    if not (1.5 <= meters <= 10):
        return None
    if not (5 <= feet <= 30):
        return None
    if not (0.4 <= sqft_mult <= 2.0):
        return None
    if not (0.4 <= cuft_mult <= 2.0):
        return None
    
    return {
        'height_meters': round(meters, 2),
        'height_feet': int(feet),
        'sqft_multiplier': round(sqft_mult, 3),
        'cuft_multiplier': round(cuft_mult, 3)
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
                'description': 'Story height multipliers - adjustments for wall heights above/below 10ft base',
                'base_height': '10 ft (3.05m) = 1.000 multiplier'
            },
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
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# STORY HEIGHT MULTIPLIERS - Section {section}\n\n")
        f.write(f"**Source:** Marshall Valuation Service, Section {section}, Page {section_page}\n")
        f.write(f"**PDF Page:** {pdf_page}\n\n")
        f.write("*Multiply base cost by these factors for wall heights other than 10 ft (3.05m)*\n\n")
        
        if multipliers:
            f.write("| Height (M.) | Height (Ft.) | Sq Ft Multiplier | Cu Ft Multiplier |\n")
            f.write("| --- | --- | --- | --- |\n")
            
            for mult in multipliers:
                meters = mult['height_meters']
                feet = mult['height_feet']
                sqft = mult['sqft_multiplier']
                cuft = mult['cuft_multiplier']
                
                # Mark the base row
                base_note = " (base)" if feet == 10 else ""
                f.write(f"| {meters:.2f} | {feet}{base_note} | {sqft:.3f} | {cuft:.3f} |\n")
        else:
            f.write("*No multipliers extracted*\n")
    
    print(f"[SAVED] Markdown: {output_file}")


def main():
    """
    Main entry point for command-line usage
    
    Usage: python parse-height-multiplier.py <pdf_path> <pdf_page>
    Example: python parse-height-multiplier.py data/pdfs/MVS.pdf 90
    """
    if len(sys.argv) < 3:
        print("Usage: python parse-height-multiplier.py <pdf_path> <pdf_page>")
        print("Example: python parse-height-multiplier.py data/pdfs/MVS.pdf 90")
        print("\nExtracts story height multiplier table from the specified PDF page.")
        print("Output files are named by section (e.g., S11_STORY_HEIGHT.md)")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    pdf_page = int(sys.argv[2])
    
    # Parse the PDF page
    results = parse_story_height_table(pdf_path, pdf_page)
    
    if results['success'] and results['section']:
        # Save output with section-based naming
        output_dir = Path(__file__).parent.parent / "Tables" / "Refinements" / "StoryHeight"
        section = results['section']
        
        # Save as markdown
        markdown_path = output_dir / f"S{section}_STORY_HEIGHT.md"
        save_to_markdown(results, str(markdown_path))
        
        # Save as JSON
        json_path = output_dir / f"S{section}_story_height.json"
        save_to_json(results, str(json_path))
        
        print(f"\n[SUCCESS] Section {section} story height multipliers extracted!")
        print(f"   Total entries: {len(results['multipliers'])}")
    else:
        print("\n[ERROR] Processing failed!")
        for error in results['errors']:
            print(f"   - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
