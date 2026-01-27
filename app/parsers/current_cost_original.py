"""
Current Cost Multiplier Parser for Marshall Valuation Service
Extracts current cost/inflation adjustment multiplier tables from MVS PDF

WAITING FOR USER INPUT:
- Section number and page range
- Table format description
- Pricing methodology details
"""

import pdfplumber
import json
import sys
from pathlib import Path
from typing import List, Dict, Optional

def parse_current_cost_multiplier_table(pdf_path: str, start_page: int, end_page: int) -> Dict:
    """
    Parse current cost multiplier tables from specified page range
    
    Args:
        pdf_path: Path to MVS PDF file
        start_page: Starting page number (1-indexed)
        end_page: Ending page number (1-indexed)
    
    Returns:
        Dictionary containing parsed multiplier data and metadata
    """
    results = {
        'success': False,
        'multipliers': [],
        'metadata': {
            'source_file': str(pdf_path),
            'pages_processed': f"{start_page}-{end_page}",
            'total_entries': 0,
        },
        'errors': []
    }
    
    try:
        print(f"[*] Opening PDF: {pdf_path}")
        print(f"[*] Processing pages {start_page} to {end_page}")
        
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            
            if start_page < 1 or end_page > total_pages:
                raise ValueError(f"Invalid page range. PDF has {total_pages} pages.")
            
            start_idx = start_page - 1
            end_idx = end_page
            
            multipliers = []
            
            for page_num in range(start_idx, end_idx):
                page = pdf.pages[page_num]
                display_page = page_num + 1
                
                print(f"\n[*] Processing page {display_page}...")
                
                page_text = page.extract_text()
                page_multipliers = parse_page_current_cost(page, page_text, display_page)
                
                if page_multipliers:
                    multipliers.extend(page_multipliers)
                    print(f"   [+] Found {len(page_multipliers)} current cost multiplier entries")
                else:
                    print(f"   [!] No multipliers found on page {display_page}")
            
            results['success'] = True
            results['multipliers'] = multipliers
            results['metadata']['total_entries'] = len(multipliers)
            
            print(f"\n[SUCCESS] Parsing complete!")
            print(f"   Total multipliers extracted: {len(multipliers)}")
            
    except FileNotFoundError:
        error_msg = f"PDF file not found: {pdf_path}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    
    return results


def parse_page_current_cost(page, page_text: str, page_num: int) -> List[Dict]:
    """
    Parse current cost multiplier entries from a single page
    
    Table structure:
    - Two side-by-side tables: Calculator (left) and Segregated (right)
    - Each section has 3 regions (Eastern, Central, Western)
    - Each region has 5 building classes (A, B, C, D, S)
    - Columns represent effective dates (month/year)
    
    Args:
        page: pdfplumber page object
        page_text: Extracted text from page
        page_num: Page number (1-indexed)
    
    Returns:
        List of multiplier dictionaries
    """
    multipliers = []
    
    # Try table extraction first
    tables = page.extract_tables()
    
    if not tables or all(not table or len(table) < 2 for table in tables):
        print(f"   [!] No valid tables found using extract_tables(), trying region-based parsing...")
        # Fallback to region-based text parsing
        return parse_region_based_multipliers(page, page_num)
    
    print(f"   Found {len(tables)} table(s) on page")
    
    # Process each table (should be 2: Calculator and Segregated)
    for table_idx, table in enumerate(tables):
        if not table or len(table) < 2:
            print(f"   [!] Skipping empty table {table_idx}")
            continue
        
        print(f"   Table {table_idx}: {len(table)} rows, {len(table[0]) if table else 0} columns")
        
        # Determine which method this is
        first_row_text = ' '.join([str(cell or '') for cell in table[0]]).upper()
        
        if 'CALCULATOR' in first_row_text:
            method = 'calculator'
        elif 'SEGREGATED' in first_row_text:
            method = 'segregated'
        else:
            # Try to infer from table position or context
            method = 'calculator' if table_idx == 0 else 'segregated'
        
        print(f"   Processing {method.upper()} table...")
        
        table_multipliers = parse_multiplier_table(table, method, page_num)
        multipliers.extend(table_multipliers)
        print(f"   Extracted {len(table_multipliers)} entries from {method} table")
    
    # If no multipliers extracted from tables, try region-based
    if not multipliers:
        print(f"   [!] No multipliers extracted from tables, trying region-based parsing...")
        multipliers = parse_region_based_multipliers(page, page_num)
    
    return multipliers


def parse_region_based_multipliers(page, page_num: int) -> List[Dict]:
    """
    Parse multipliers by splitting page into left (Calculator) and right (Segregated) regions
    
    Args:
        page: pdfplumber page object
        page_num: Page number (1-indexed)
    
    Returns:
        List of multiplier dictionaries
    """
    import re
    multipliers = []
    
    # Get page dimensions
    page_width = page.width
    page_height = page.height
    
    # Split at 55% instead of 50% - Calculator table is wider
    split_point = page_width * 0.55
    
    # Define bounding boxes for left (Calculator) and right (Segregated) sections
    left_bbox = (0, 0, split_point, page_height)
    # Test expanded bbox for segregated section to capture missing Western A/B
    right_bbox = (420.0, 0.0, 792.0, 612.0)  # Expanded left boundary from 435.6 to 420.0
    
    # Parse Calculator section (left half)
    print(f"   Parsing CALCULATOR section (left half)...")
    print(f"   Left bbox: x0={left_bbox[0]:.1f}, y0={left_bbox[1]:.1f}, x1={left_bbox[2]:.1f}, y1={left_bbox[3]:.1f}")
    left_text = page.within_bbox(left_bbox).extract_text()
    if left_text:
        print(f"   Left text length: {len(left_text)} chars, first 100: {left_text[:100]}")
        calc_multipliers = parse_single_table_text(left_text, 'calculator', page_num)
        multipliers.extend(calc_multipliers)
        print(f"   Extracted {len(calc_multipliers)} calculator entries")
    else:
        print(f"   [!] No text extracted from left half")
    
    # Parse Segregated section (right half)
    print(f"   Parsing SEGREGATED section (right half)...")
    print(f"   Right bbox: x0={right_bbox[0]:.1f}, y0={right_bbox[1]:.1f}, x1={right_bbox[2]:.1f}, y1={right_bbox[3]:.1f}")
    right_text = page.within_bbox(right_bbox).extract_text()
    if right_text:
        print(f"   Right text length: {len(right_text)} chars, first 100: {right_text[:100]}")
        seg_multipliers = parse_single_table_text(right_text, 'segregated', page_num)
        multipliers.extend(seg_multipliers)
        print(f"   Extracted {len(seg_multipliers)} segregated entries")
    else:
        print(f"   [!] No text extracted from right half")
    
    return multipliers


def parse_single_table_text(text: str, method: str, page_num: int) -> List[Dict]:
    """
    Parse a single table's text content (either Calculator or Segregated)
    
    Args:
        text: Extracted text from the table region
        method: 'calculator' or 'segregated'
        page_num: Page number for reference
    
    Returns:
        List of multiplier dictionaries
    """
    import re
    multipliers = []
    
    lines = text.split('\n')
    current_region = None
    effective_dates = []
    in_data_section = False
    
    # First pass: accumulate all date headers before processing data
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        line_upper = line.upper()
        
        # Stop accumulating dates once we hit region headers or data
        if any(kw in line_upper for kw in ['EASTERN', 'CENTRAL', 'WESTERN']) and effective_dates:
            break
        
        # Look for dates in parentheses format
        paren_date_pattern = r'\((\d{1,2})/(\d{2,4})\)'
        if re.search(paren_date_pattern, line):
            date_matches = re.findall(paren_date_pattern, line)
            for month, year in date_matches:
                # Normalize year
                if len(year) == 2:
                    year = '20' + year if int(year) < 50 else '19' + year
                date_str = f"{int(month)}/{year}"
                if date_str not in effective_dates:
                    effective_dates.append(date_str)
    
    if effective_dates:
        print(f"      Found {len(effective_dates)} effective dates: {', '.join(effective_dates[:4])}...")
        in_data_section = True
    
    # Helper function to look ahead for next region
    def find_next_region(start_idx, lines):
        """Look ahead to find the next region label"""
        for i in range(start_idx + 1, len(lines)):
            line_upper = lines[i].upper()
            if 'EASTERN' in line_upper and 'EFFECTIVE' not in line_upper and not ('CENTRAL' in line_upper and 'WESTERN' in line_upper):
                return 'Eastern'
            elif 'CENTRAL' in line_upper and 'EFFECTIVE' not in line_upper and not ('EASTERN' in line_upper and 'WESTERN' in line_upper):
                return 'Central'
            elif 'WESTERN' in line_upper and 'EFFECTIVE' not in line_upper and not ('EASTERN' in line_upper and 'CENTRAL' in line_upper):
                return 'Western'
        return None
    
    # Second pass: process data rows
    for idx, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        
        line_upper = line.upper()
        
        # Skip header rows
        if any(keyword in line_upper for keyword in ['CLASS', 'MULTIPLIER', 'SECTION', 'APPLY', 'COST', 'EFFECTIVE', 'PAGES']):
            continue
        
        # Skip rows that are just numbers (cost page numbers like "41 42 43 44")
        if re.match(r'^[\d\s]+$', line) and len(line.split()) > 3:
            continue
        
        # Check if this line contains a region label
        # Format: "EASTERN  C  1.06  1.06  1.06..." (region + data on same line)
        # Exclude lines that contain all three regions (header rows)
        region_match = None
        if ('EASTERN' in line_upper and 'CENTRAL' in line_upper and 'WESTERN' in line_upper):
            # Skip header rows that contain all region names
            continue
        elif 'EASTERN' in line_upper and 'EFFECTIVE' not in line_upper:
            region_match = 'Eastern'
        elif 'CENTRAL' in line_upper and 'EFFECTIVE' not in line_upper:
            region_match = 'Central'
        elif 'WESTERN' in line_upper and 'EFFECTIVE' not in line_upper:
            region_match = 'Western'
        
        if region_match:
            current_region = region_match
            print(f"      Found {current_region} region")
            # Remove the region label from the line
            line = re.sub(r'(EASTERN|CENTRAL|WESTERN)', '', line, flags=re.IGNORECASE).strip()
            # If there's no content left after removing region label, skip this line
            if not line:
                continue
            # Reparse line after removing region label
            line_upper = line.upper()
        
        # Extract data rows
        if not effective_dates or not in_data_section:
            continue
        
        # Data row format: "A  1.06  1.06  1.06..."
        parts = line.split()
        
        # Must start with a building class letter
        if not parts or parts[0] not in ['A', 'B', 'C', 'D', 'S']:
            continue
        
        # Special handling for A and B classes that appear before region labels
        # If we see A or B and current_region is not set OR if previous line was S/D,
        # look ahead to find which region these belong to
        building_class = parts[0]
        target_region = current_region
        
        if building_class in ['A', 'B']:
            # Check if we need to look ahead for the region
            # This happens when A/B appear before the region label
            if current_region is None or (idx > 0 and any(
                lines[i].strip().startswith(('S ', 'D ')) for i in range(max(0, idx-3), idx)
            )):
                next_region = find_next_region(idx, lines)
                if next_region:
                    target_region = next_region
                    print(f"      Look-ahead: Attributing {building_class} to {target_region} (next region)")
        
        if not target_region:
            continue
        
        building_class = parts[0]
        value_parts = parts[1:]
        
        # Extract numeric values (multipliers)
        multiplier_values = []
        for part in value_parts:
            # Clean part and try to parse
            clean_part = part.strip('()').replace(',', '')
            
            # Skip if it looks like a label or text
            if any(c.isalpha() for c in clean_part):
                continue
            
            try:
                val = float(clean_part)
                # Accept reasonable multiplier values
                if 0.5 <= val <= 2.0:
                    multiplier_values.append(val)
            except ValueError:
                continue
        
        # Debug: Show mismatch if any
        if multiplier_values and target_region:
            if len(multiplier_values) != len(effective_dates):
                print(f"      [!] Class {building_class} in {target_region}: Found {len(multiplier_values)} values but {len(effective_dates)} dates")
            
            # Match multipliers to dates
            num_entries = min(len(multiplier_values), len(effective_dates))
            
            for idx in range(num_entries):
                mult_val = multiplier_values[idx]
                effective_date = effective_dates[idx]
                
                multipliers.append({
                    'method': method,
                    'region': target_region,
                    'building_class': building_class,
                    'effective_date': effective_date,
                    'multiplier': mult_val,
                    'source_page': page_num
                })
    
    return multipliers


def parse_multiplier_table(table: List[List[str]], method: str, page_num: int) -> List[Dict]:
    """
    Parse a single multiplier table (Calculator or Segregated)
    
    Args:
        table: Extracted table data
        method: 'calculator' or 'segregated'
        page_num: Source page number
    
    Returns:
        List of multiplier dictionaries
    """
    multipliers = []
    current_region = None
    effective_dates = []
    
    for row_idx, row in enumerate(table):
        if not row or not any(row):
            continue
        
        # Clean the row
        cleaned = [str(cell).strip() if cell else '' for cell in row]
        row_text = ' '.join(cleaned).upper()
        
        # Skip empty or header-only rows
        if not any(cleaned):
            continue
        
        # Check if this is an effective date header row
        if 'EFFECTIVE DATE' in row_text or any('/' in str(cell) for cell in cleaned if cell):
            # This row might contain effective dates
            # Extract dates from cells that look like "(11/24)" or "11/24"
            import re
            for cell in cleaned:
                if cell:
                    # Look for month/year patterns
                    date_match = re.search(r'\(?(\d{1,2})/(\d{2,4})\)?', cell)
                    if date_match:
                        month = date_match.group(1)
                        year = date_match.group(2)
                        # Convert 2-digit year to 4-digit if needed
                        if len(year) == 2:
                            year = '20' + year if int(year) < 50 else '19' + year
                        effective_dates.append(f"{month}/{year}")
            continue
        
        # Check if this is a region header
        if 'EASTERN' in row_text:
            current_region = 'Eastern'
            continue
        elif 'CENTRAL' in row_text:
            current_region = 'Central'
            continue
        elif 'WESTERN' in row_text:
            current_region = 'Western'
            continue
        
        # Skip other header rows
        if any(keyword in row_text for keyword in ['CLASS', 'MULTIPLIER', 'SECTION', 'UNIT-IN-PLACE']):
            continue
        
        # Check if this row has a building class
        building_class = None
        if cleaned and cleaned[0] in ['A', 'B', 'C', 'D', 'S']:
            building_class = cleaned[0]
        
        if building_class and current_region:
            # Extract multiplier values from this row
            multiplier_values = []
            for cell in cleaned[1:]:  # Skip the class column
                if cell:
                    try:
                        val = float(cell)
                        # Multipliers should be in reasonable range (0.8 to 1.2)
                        if 0.8 <= val <= 1.2:
                            multiplier_values.append(val)
                    except ValueError:
                        continue
            
            # Create entries for each effective date
            for idx, multiplier_val in enumerate(multiplier_values):
                if idx < len(effective_dates):
                    effective_date = effective_dates[idx]
                else:
                    # If we don't have enough dates, use index as placeholder
                    effective_date = f"Column_{idx+1}"
                
                multipliers.append({
                    'method': method,
                    'region': current_region,
                    'building_class': building_class,
                    'effective_date': effective_date,
                    'multiplier': multiplier_val,
                    'source_page': page_num
                })
    
    return multipliers


def get_region_for_state(state: str) -> str:
    """
    Map US state to region (Eastern, Central, Western) based on district map
    
    Args:
        state: Two-letter state code or full state name
    
    Returns:
        Region name: 'Eastern', 'Central', or 'Western'
    """
    # Based on the district map from Section 99
    eastern_states = [
        'ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE', 'MD', 
        'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'DC'
    ]
    
    western_states = [
        'WA', 'OR', 'CA', 'NV', 'ID', 'MT', 'WY', 'UT', 'CO', 'AZ', 'NM', 
        'AK', 'HI'
    ]
    
    # Central is everything else
    central_states = [
        'ND', 'SD', 'NE', 'KS', 'OK', 'TX', 'MN', 'IA', 'MO', 'AR', 'LA',
        'WI', 'IL', 'MI', 'IN', 'OH', 'KY', 'TN', 'MS', 'AL'
    ]
    
    state_upper = state.upper()
    
    if state_upper in eastern_states:
        return 'Eastern'
    elif state_upper in western_states:
        return 'Western'
    elif state_upper in central_states:
        return 'Central'
    else:
        # Default to Central if unknown
        return 'Central'


def save_to_json(multipliers: List[Dict], output_path: str) -> None:
    """Save parsed multipliers to JSON file"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Calculate statistics
    methods = set(m['method'] for m in multipliers)
    regions = set(m['region'] for m in multipliers)
    classes = set(m['building_class'] for m in multipliers)
    dates = set(m['effective_date'] for m in multipliers)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'source': 'Marshall Valuation Service - Section 99, Page 3 (PDF Page 717)',
                'description': 'Current cost multipliers for bringing costs up to date',
                'methods': sorted(list(methods)),
                'regions': sorted(list(regions)),
                'building_classes': sorted(list(classes)),
                'effective_dates': sorted(list(dates)),
                'total_entries': len(multipliers),
                'note': 'Use get_region_for_state() to map state codes to regions'
            },
            'region_mapping': {
                'Eastern': ['ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE', 'MD', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'DC'],
                'Central': ['ND', 'SD', 'NE', 'KS', 'OK', 'TX', 'MN', 'IA', 'MO', 'AR', 'LA', 'WI', 'IL', 'MI', 'IN', 'OH', 'KY', 'TN', 'MS', 'AL'],
                'Western': ['WA', 'OR', 'CA', 'NV', 'ID', 'MT', 'WY', 'UT', 'CO', 'AZ', 'NM', 'AK', 'HI']
            },
            'multipliers': multipliers
        }, f, indent=2)
    
    print(f"[SAVED] JSON: {output_file}")
    print(f"   Methods: {', '.join(sorted(methods))}")
    print(f"   Regions: {', '.join(sorted(regions))}")
    print(f"   Effective dates: {len(dates)}")


def save_to_markdown(multipliers: List[Dict], output_path: str) -> None:
    """Save parsed multipliers to markdown file"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# CURRENT COST MULTIPLIERS\n\n")
        f.write("**Source:** Marshall Valuation Service - Section 99, Page 3 (PDF Page 717)\n")
        f.write(f"**Total Entries:** {len(multipliers)}\n\n")
        
        f.write("## About Current Cost Multipliers\n\n")
        f.write("These multipliers bring costs from preceding pages up to date. They account for:\n")
        f.write("- Construction cost inflation\n")
        f.write("- Material price changes\n")
        f.write("- Labor rate adjustments\n\n")
        f.write("**Also apply Local Multipliers** from Section 99, Pages 5-10.\n\n")
        
        f.write("## Regional District Map\n\n")
        f.write("- **Eastern:** ME, NH, VT, MA, RI, CT, NY, NJ, PA, DE, MD, VA, WV, NC, SC, GA, FL, DC\n")
        f.write("- **Central:** ND, SD, NE, KS, OK, TX, MN, IA, MO, AR, LA, WI, IL, MI, IN, OH, KY, TN, MS, AL\n")
        f.write("- **Western:** WA, OR, CA, NV, ID, MT, WY, UT, CO, AZ, NM, AK, HI\n\n")
        
        if multipliers:
            # Group by method
            by_method = {}
            for mult in multipliers:
                method = mult.get('method', 'unknown')
                if method not in by_method:
                    by_method[method] = []
                by_method[method].append(mult)
            
            for method, entries in sorted(by_method.items()):
                f.write(f"## {method.upper()} Cost Sections\n\n")
                
                # Group by region within each method
                by_region = {}
                for mult in entries:
                    region = mult.get('region', 'unknown')
                    if region not in by_region:
                        by_region[region] = []
                    by_region[region].append(mult)
                
                for region in ['Eastern', 'Central', 'Western']:
                    if region in by_region:
                        f.write(f"### {region} Region\n\n")
                        f.write("| Class | Effective Date | Multiplier | Page |\n")
                        f.write("| --- | --- | --- | --- |\n")
                        
                        for mult in sorted(by_region[region], key=lambda x: (x['building_class'], x['effective_date'])):
                            bldg_class = mult.get('building_class', '')
                            date = mult.get('effective_date', '')
                            multiplier = mult.get('multiplier', '')
                            page = mult.get('source_page', '')
                            
                            f.write(f"| {bldg_class} | {date} | {multiplier} | {page} |\n")
                        
                        f.write("\n")
        else:
            f.write("*No multipliers extracted*\n")
    
    print(f"[SAVED] Markdown: {output_file}")


def main():
    if len(sys.argv) < 4:
        print("Usage: python parse-current-cost-multiplier.py <pdf_path> <start_page> <end_page>")
        print("Example: python parse-current-cost-multiplier.py MVS.pdf 717 717")
        print("\nParser is ready! Extract from Section 99, Page 3 (PDF page 717)")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    start_page = int(sys.argv[2])
    end_page = int(sys.argv[3])
    
    results = parse_current_cost_multiplier_table(pdf_path, start_page, end_page)
    
    if results['success']:
        output_dir = Path(__file__).parent.parent / "Tables" / "Multipliers" / "CurrentCost"
        
        markdown_path = output_dir / "CURRENT_COST_MULTIPLIERS.md"
        save_to_markdown(results['multipliers'], str(markdown_path))
        
        json_path = output_dir / "current_cost_multipliers.json"
        save_to_json(results['multipliers'], str(json_path))
        
        print("\n[SUCCESS] Processing complete!")
    else:
        print("\n[ERROR] Processing failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
