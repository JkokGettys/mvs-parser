"""
Local Multiplier Parser for Marshall Valuation Service
Extracts local/regional cost multiplier tables from MVS PDF

This parser is designed to be robust across quarterly PDF updates.
Pages 719-724 in current PDF (may vary by version).
"""

import pdfplumber
import json
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional

def parse_local_multiplier_table(pdf_path: str, start_page: int, end_page: int) -> Dict:
    """
    Parse local multiplier tables from specified page range
    
    Args:
        pdf_path: Path to MVS PDF file
        start_page: Starting page number (1-indexed for user, converted to 0-indexed)
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
            
            # Validate page range
            if start_page < 1 or end_page > total_pages:
                raise ValueError(f"Invalid page range. PDF has {total_pages} pages.")
            
            # Convert to 0-indexed
            start_idx = start_page - 1
            end_idx = end_page
            
            multipliers = []
            
            for page_num in range(start_idx, end_idx):
                page = pdf.pages[page_num]
                display_page = page_num + 1
                
                print(f"\n[*] Processing page {display_page}...")
                
                # Extract text for analysis
                page_text = page.extract_text()
                
                # TODO: Implement table parsing logic
                # This will be filled in after you provide sample data
                page_multipliers = parse_page_multipliers(page, page_text, display_page)
                
                if page_multipliers:
                    multipliers.extend(page_multipliers)
                    print(f"   [+] Found {len(page_multipliers)} multiplier entries")
                else:
                    print(f"   [!] No multipliers found on page {display_page}")
            
            # Store results
            results['success'] = True
            results['multipliers'] = multipliers
            results['metadata']['total_entries'] = len(multipliers)
            
            print(f"\n[SUCCESS] Parsing complete!")
            print(f"   Total multipliers extracted: {len(multipliers)}")
            
    except FileNotFoundError:
        error_msg = f"PDF file not found: {pdf_path}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    except ValueError as e:
        error_msg = str(e)
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        results['errors'].append(error_msg)
    
    return results


def parse_page_multipliers(page, page_text: str, page_num: int) -> List[Dict]:
    """
    Parse multiplier entries from a single page
    
    The table structure is:
    - PROVINCE/TERRITORY name (all caps) followed by 5 multipliers (A, B, C, D, S)
    - City names (mixed case) followed by 5 multipliers
    - Multiple regions on same page (Canada, US Territories, etc.)
    
    Args:
        page: pdfplumber page object
        page_text: Extracted text from page
        page_num: Page number (1-indexed)
    
    Returns:
        List of multiplier dictionaries
    """
    multipliers = []
    
    # Try table extraction with more lenient settings
    table_settings = {
        "vertical_strategy": "lines_strict",
        "horizontal_strategy": "lines_strict",
        "explicit_vertical_lines": [],
        "explicit_horizontal_lines": [],
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "edge_min_length": 3,
        "min_words_vertical": 1,
        "min_words_horizontal": 1,
        "intersection_tolerance": 3,
    }
    
    tables = page.extract_tables(table_settings)
    
    if tables and any(len(table) > 3 for table in tables):  # At least one table with data
        print(f"   Found {len(tables)} table(s) on page")
        for table_idx, table in enumerate(tables):
            table_multipliers = parse_table_data(table, page_num)
            multipliers.extend(table_multipliers)
            print(f"   Table {table_idx + 1}: Extracted {len(table_multipliers)} entries")
    else:
        # Use position-based text extraction to preserve spatial layout
        print(f"   Using position-based text extraction")
        multipliers = parse_position_based_text(page, page_num)
    
    return multipliers


def parse_table_data(table: List[List[str]], page_num: int) -> List[Dict]:
    """
    Parse multipliers from extracted table data
    
    Expected format:
    [
        ['ALBERTA', '1.21', '1.29', '1.26', '1.19', '1.21'],
        ['Calgary', '1.21', '1.30', '1.27', '1.20', '1.21'],
        ...
    ]
    """
    multipliers = []
    current_region = None
    current_country = "Unknown"
    
    # Determine country from context (check for CANADA, USA, etc. in headers)
    for row in table[:5]:  # Check first few rows for headers
        if row and any(cell for cell in row if cell):
            row_text = ' '.join([str(c or '') for c in row]).upper()
            if 'CANADA' in row_text:
                current_country = "Canada"
            elif 'UNITED STATES' in row_text or 'U.S.' in row_text:
                current_country = "United States"
    
    for row in table:
        if not row or not any(row):
            continue
        
        # Clean the row
        cleaned = [str(cell).strip() if cell else '' for cell in row]
        
        # Join row text for exclusion checks
        row_text = ' '.join(cleaned).upper()
        
        # Skip header rows
        if any(keyword in row_text for keyword in ['CLASS', 'MULTIPLIER', 'APPLY TO']):
            continue
        
        # Skip tax removal table rows (contains percentages)
        if any(keyword in row_text for keyword in ['TAX REMOVAL', 'DEDUCTION', 'EXAMPLE:']):
            continue
        
        # Skip rows with percentage signs (tax table)
        if '%' in row_text or any('%' in str(cell) for cell in cleaned):
            continue
        
        # Look for location name and multiplier values
        # Format: [Location, A, B, C, D, S] or [Location, ..., A, B, C, D, S]
        location_name = cleaned[0] if cleaned else ''
        
        if not location_name:
            continue
        
        # Extract multiplier values (last 5 columns should be A, B, C, D, S)
        multiplier_values = []
        for cell in cleaned[1:]:
            try:
                # Skip cells with percentage signs
                if '%' in str(cell):
                    continue
                    
                # Try to convert to float - these are the multiplier values
                val = float(cell)
                
                # Multipliers should be between 0.5 and 2.5 (reasonable range)
                # Tax percentages would be 5.0+, so exclude those
                if 0.5 <= val <= 2.5:
                    multiplier_values.append(val)
            except (ValueError, TypeError):
                # Not a number, might be part of location name or other text
                continue
        
        # We expect exactly 5 multipliers (A, B, C, D, S)
        if len(multiplier_values) >= 5:
            # Take the last 5 values
            class_multipliers = multiplier_values[-5:]
            
            # Determine if this is a region/province or a city
            is_region = location_name.isupper() or location_name in ['ALBERTA', 'BRITISH COLUMBIA', 'ONTARIO', 'QUEBEC', 'MANITOBA', 'SASKATCHEWAN', 'YUKON', 'NORTHWEST TERRITORY', 'GUAM', 'PUERTO RICO', 'VIRGIN ISLANDS']
            
            if is_region:
                current_region = location_name
                # Add region-level entry
                multipliers.append({
                    'location': location_name,
                    'city': None,
                    'region': location_name,
                    'country': current_country,
                    'class_a': class_multipliers[0],
                    'class_b': class_multipliers[1],
                    'class_c': class_multipliers[2],
                    'class_d': class_multipliers[3],
                    'class_s': class_multipliers[4],
                    'source_page': page_num,
                    'is_regional': True,
                })
            else:
                # This is a city under the current region
                multipliers.append({
                    'location': f"{location_name}, {current_region}" if current_region else location_name,
                    'city': location_name,
                    'region': current_region,
                    'country': current_country,
                    'class_a': class_multipliers[0],
                    'class_b': class_multipliers[1],
                    'class_c': class_multipliers[2],
                    'class_d': class_multipliers[3],
                    'class_s': class_multipliers[4],
                    'source_page': page_num,
                    'is_regional': False,
                })
    
    return multipliers


def clean_text_spacing(text: str) -> str:
    """
    Clean up extra spaces that appear in PDF text extraction
    Examples: "Y armouth" -> "Yarmouth", "M ARITIMES" -> "MARITIMES"
    Also separates text from numbers: "TERRITORY1.53" -> "TERRITORY 1.53"
    Joins broken numbers: "1 .01" -> "1.01", "0 .96" -> "0.96"
    """
    import re
    # Remove spaces between single capital letters and following text
    # This handles cases like "M ARITIMES", "Y armouth", etc.
    text = re.sub(r'\b([A-Z])\s+([a-z])', r'\1\2', text)
    text = re.sub(r'\b([A-Z])\s+([A-Z])', r'\1\2', text)
    
    # Join broken decimal numbers: "1 .01" -> "1.01", "0 .96" -> "0.96"
    # This handles PDF extraction issues where numbers are split
    text = re.sub(r'(\d)\s+(\.\d+)', r'\1\2', text)
    
    # Join broken decimal numbers with period-space: "1. 02" -> "1.02"
    # This handles another PDF extraction pattern
    text = re.sub(r'(\d\.)\s+(\d+)', r'\1\2', text)
    
    # Separate text from numbers that are stuck together
    # e.g., "TERRITORY1.53" -> "TERRITORY 1.53"
    text = re.sub(r'([A-Za-z])(\d)', r'\1 \2', text)
    
    return text


def get_country_for_region(region_name: str, default_country: str) -> str:
    """
    Determine the correct country for a region, handling special cases like US territories
    """
    us_territories = ['GUAM', 'PUERTO RICO', 'VIRGIN ISLANDS', 'VIRGIN ISLANDS (U.S.)']
    
    if any(territory in region_name.upper() for territory in us_territories):
        return "United States"
    
    return default_country


def parse_position_based_text(page, page_num: int) -> List[Dict]:
    """
    Parse using cropped regions to handle multi-column layouts
    Divides page into 3 equal columns and parses each independently
    
    Args:
        page: pdfplumber page object
        page_num: Page number (1-indexed)
    
    Returns:
        List of multiplier dictionaries
    """
    multipliers = []
    
    # Determine country from page text
    page_text = page.extract_text()
    if 'CANADA' in page_text.upper():
        default_country = "Canada"
    elif 'UNITED STATES' in page_text.upper():
        default_country = "United States"
    else:
        default_country = "Unknown"
    
    # Get page dimensions
    page_width = page.width
    page_height = page.height
    
    # Define 3 columns (typical MVS layout)
    num_columns = 3
    column_width = page_width / num_columns
    
    # Track current region across columns (states can continue across columns)
    current_region = None
    parent_region = None  # Track parent region for subsections like "NEW YORK CITY AREA"
    
    # Process each column
    for col_idx in range(num_columns):
        # Use crop-based extraction with adjusted boundaries
        # For columns 2 and 3, shift left boundary to capture first letters
        # 6 pixels is enough to capture cut-off letters without too much overlap
        left_shift = 6 if col_idx > 0 else 0
        
        x0 = (col_idx * column_width) - left_shift
        x1 = (col_idx + 1) * column_width
        y0 = 0
        y1 = page_height
        
        # Crop to this column
        cropped = page.crop((x0, y0, x1, y1))
        column_text = cropped.extract_text()
        
        if not column_text:
            continue
        
        # Parse this column's text line by line
        lines = column_text.split('\n')
        
        for line_idx, line in enumerate(lines):
            line = line.strip()
            
            if not line:
                continue
            
            # Clean up spacing issues
            line = clean_text_spacing(line)
            
            # Skip headers and tax table
            # Use word boundary matching for short keywords to avoid false matches (e.g., "Kingston" contains "GST")
            line_upper = line.upper()
            import re
            skip = False
            
            # Check for exact phrases or start-of-line matches
            if any(phrase in line_upper for phrase in [
                'LOCAL MULTIPLIER', 'SECTION 99', 'MARSHALL', 'APPLY TO',
                'TAX REMOVAL', 'DEDUCTION', 'EXAMPLE:', 'CANADA', 'UNITED STATES'
            ]):
                skip = True
            
            # Check for whole word matches for short keywords using word boundaries
            if re.search(r'\b(GST|PST|HST|CLASS|PAGE)\b', line_upper):
                skip = True
            
            if skip:
                continue
            
            # Skip lines with percentages
            if '%' in line:
                continue
            
            # Try to extract location name and multipliers
            parts = line.split()
            
            if len(parts) < 6:
                # Check if this might be a region header without multipliers (edge case)
                # This can happen when a region header appears alone
                cleaned_line = clean_text_spacing(' '.join(parts))
                if cleaned_line.isupper() and len(cleaned_line) > 5:
                    # Look ahead to see if the next line has the multipliers
                    if line_idx + 1 < len(lines):
                        next_line = lines[line_idx + 1].strip()
                        next_parts = next_line.split()
                        # Check if next line is just numbers
                        if len(next_parts) >= 5:
                            try:
                                next_values = [float(p) for p in next_parts if 0.5 <= float(p) <= 2.5]
                                if len(next_values) >= 5:
                                    # This is a region header split across lines
                                    current_region = cleaned_line
                                    entry_country = get_country_for_region(cleaned_line, default_country)
                                    multipliers.append({
                                        'location': cleaned_line,
                                        'city': None,
                                        'region': cleaned_line,
                                        'country': entry_country,
                                        'class_a': next_values[-5],
                                        'class_b': next_values[-4],
                                        'class_c': next_values[-3],
                                        'class_d': next_values[-2],
                                        'class_s': next_values[-1],
                                        'source_page': page_num,
                                        'is_regional': True,
                                    })
                                    continue
                            except (ValueError, IndexError):
                                pass
                continue
            
            # Extract numeric values (multipliers)
            # For columns 2 and 3 with left shift, we may pick up trailing numbers from previous column
            # Strategy: collect all numbers and text, then use only the LAST 5 numbers and text that comes before them
            multiplier_values = []
            location_parts = []
            has_seen_text = False
            
            for part in parts:
                try:
                    if '%' in part:
                        continue
                    val = float(part)
                    if 0.5 <= val <= 2.5:
                        # Only collect multipliers that appear after we've seen text
                        # This helps skip trailing numbers from previous column
                        if has_seen_text or len(location_parts) > 0:
                            multiplier_values.append(val)
                        # If we haven't seen text yet and this looks like a stray number, skip it
                except ValueError:
                    # This is text (location name part)
                    has_seen_text = True
                    # Only collect location parts before we have 5 multipliers
                    if len(multiplier_values) < 5:
                        location_parts.append(part)
            
            if len(multiplier_values) >= 5:
                location_name = ' '.join(location_parts).strip()
                
                # Clean up the location name
                location_name = clean_text_spacing(location_name)
                
                # If we have more than 5 multipliers (due to column overlap), use only the last 5
                # This handles cases where we pick up trailing numbers from the previous column
                if len(multiplier_values) > 5:
                    multiplier_values = multiplier_values[-5:]
                
                # Skip if location is empty or too short
                if not location_name or len(location_name) < 3:
                    continue
                
                class_multipliers = multiplier_values[-5:]  # Last 5 values
                
                # Determine if region or city
                is_region = location_name.isupper()
                
                # Handle "(Continued)" suffix in region names
                clean_location_name = location_name.replace('(CONTINUED)', '').replace('(Continued)', '').strip()
                
                # Determine the country (handles US territories)
                entry_country = get_country_for_region(clean_location_name, default_country)
                
                if is_region:
                    # Check if this is a subsection (e.g., "NEW YORK CITY AREA")
                    # Subsections typically contain keywords like "AREA", "COUNTY", etc. and are still in parent state
                    is_subsection = any(keyword in clean_location_name for keyword in ['AREA', 'REGION', 'DISTRICT'])
                    
                    if is_subsection and current_region and current_region != clean_location_name:
                        # This is a subsection within a parent region (e.g., "NEW YORK CITY AREA" within "NEW YORK")
                        parent_region = current_region
                        current_region = clean_location_name
                    else:
                        # This is a main region/state
                        parent_region = None
                        current_region = clean_location_name
                    
                    multipliers.append({
                        'location': clean_location_name,
                        'city': None,
                        'region': clean_location_name,
                        'country': entry_country,
                        'class_a': class_multipliers[0],
                        'class_b': class_multipliers[1],
                        'class_c': class_multipliers[2],
                        'class_d': class_multipliers[3],
                        'class_s': class_multipliers[4],
                        'source_page': page_num,
                        'is_regional': True,
                    })
                else:
                    # For cities, also check if they belong to a US territory
                    city_country = get_country_for_region(current_region if current_region else "", entry_country)
                    
                    multipliers.append({
                        'location': f"{location_name}, {current_region}" if current_region else location_name,
                        'city': location_name,
                        'region': current_region,
                        'country': city_country,
                        'class_a': class_multipliers[0],
                        'class_b': class_multipliers[1],
                        'class_c': class_multipliers[2],
                        'class_d': class_multipliers[3],
                        'class_s': class_multipliers[4],
                        'source_page': page_num,
                        'is_regional': False,
                    })
    
    return multipliers


def parse_text_data(page_text: str, page_num: int) -> List[Dict]:
    """
    Fallback text-based parsing if table extraction fails
    
    Looks for patterns like:
    ALBERTA 1.21 1.29 1.26 1.19 1.21
    Calgary 1.21 1.30 1.27 1.20 1.21
    """
    multipliers = []
    current_region = None
    current_country = "Unknown"
    
    # Determine country
    if 'CANADA' in page_text.upper():
        current_country = "Canada"
    elif 'UNITED STATES' in page_text.upper():
        current_country = "United States"
    
    lines = page_text.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Skip header/footer lines
        if any(keyword in line.upper() for keyword in ['LOCAL MULTIPLIER', 'SECTION 99', 'PAGE', 'MARSHALL', 'APPLY TO']):
            continue
        
        # Skip tax removal table lines
        if any(keyword in line.upper() for keyword in ['TAX REMOVAL', 'DEDUCTION', 'EXAMPLE:', 'GST', 'PST', 'HST']):
            continue
        
        # Skip lines with percentage signs
        if '%' in line:
            continue
        
        # Try to extract location and multipliers
        # Pattern: Location_Name 1.XX 1.XX 1.XX 1.XX 1.XX
        parts = line.split()
        
        # Need at least location name + 5 multipliers
        if len(parts) < 6:
            continue
        
        # Try to find 5 consecutive float values (the multipliers)
        multiplier_values = []
        location_parts = []
        
        for part in parts:
            try:
                # Skip percentage values
                if '%' in part:
                    continue
                    
                val = float(part)
                
                # Only accept reasonable multiplier values (0.5 to 2.5)
                if 0.5 <= val <= 2.5:
                    multiplier_values.append(val)
            except ValueError:
                if len(multiplier_values) < 5:
                    location_parts.append(part)
        
        if len(multiplier_values) >= 5:
            location_name = ' '.join(location_parts)
            class_multipliers = multiplier_values[-5:]  # Last 5 values
            
            # Determine if region or city
            is_region = location_name.isupper()
            
            if is_region:
                current_region = location_name
                multipliers.append({
                    'location': location_name,
                    'city': None,
                    'region': location_name,
                    'country': current_country,
                    'class_a': class_multipliers[0],
                    'class_b': class_multipliers[1],
                    'class_c': class_multipliers[2],
                    'class_d': class_multipliers[3],
                    'class_s': class_multipliers[4],
                    'source_page': page_num,
                    'is_regional': True,
                })
            else:
                multipliers.append({
                    'location': f"{location_name}, {current_region}" if current_region else location_name,
                    'city': location_name,
                    'region': current_region,
                    'country': current_country,
                    'class_a': class_multipliers[0],
                    'class_b': class_multipliers[1],
                    'class_c': class_multipliers[2],
                    'class_d': class_multipliers[3],
                    'class_s': class_multipliers[4],
                    'source_page': page_num,
                    'is_regional': False,
                })
    
    return multipliers


def save_to_markdown(multipliers: List[Dict], output_path: str) -> None:
    """
    Save parsed multipliers to markdown file
    
    Args:
        multipliers: List of multiplier dictionaries
        output_path: Path to output markdown file
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# LOCAL MULTIPLIERS\n\n")
        f.write("**Source:** Marshall Valuation Service - Section 99\n")
        f.write(f"**Total Entries:** {len(multipliers)}\n")
        f.write(f"**Last Updated:** July 2025\n\n")
        
        f.write("## About Local Multipliers\n\n")
        f.write("Local multipliers adjust base construction costs for regional variations in:\n")
        f.write("- Labor costs\n")
        f.write("- Material availability and pricing\n")
        f.write("- Local building codes and requirements\n")
        f.write("- Market conditions\n\n")
        
        f.write("Apply these multipliers to costs brought up-to-date from base cost tables.\n\n")
        
        f.write("## Multiplier Table\n\n")
        
        if multipliers:
            # Group by country for better organization
            by_country = {}
            for mult in multipliers:
                country = mult.get('country', 'Unknown')
                if country not in by_country:
                    by_country[country] = []
                by_country[country].append(mult)
            
            for country, entries in by_country.items():
                f.write(f"### {country}\n\n")
                f.write("| Location | Region | Class A | Class B | Class C | Class D | Class S | Page |\n")
                f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
                
                for mult in entries:
                    location = mult.get('location', 'Unknown')
                    region = mult.get('region', '')
                    class_a = mult.get('class_a', '')
                    class_b = mult.get('class_b', '')
                    class_c = mult.get('class_c', '')
                    class_d = mult.get('class_d', '')
                    class_s = mult.get('class_s', '')
                    page = mult.get('source_page', '')
                    
                    # Format location (bold if regional)
                    if mult.get('is_regional'):
                        location = f"**{location}**"
                    
                    f.write(f"| {location} | {region} | {class_a} | {class_b} | {class_c} | {class_d} | {class_s} | {page} |\n")
                
                f.write("\n")
        else:
            f.write("*No multipliers extracted*\n")
    
    print(f"[SAVED] Markdown: {output_file}")


def save_to_json(multipliers: List[Dict], output_path: str) -> None:
    """
    Save parsed multipliers to JSON file for potential database import
    
    Args:
        multipliers: List of multiplier dictionaries
        output_path: Path to output JSON file
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Count statistics
    total = len(multipliers)
    by_country = {}
    regional_count = sum(1 for m in multipliers if m.get('is_regional'))
    city_count = total - regional_count
    
    for mult in multipliers:
        country = mult.get('country', 'Unknown')
        by_country[country] = by_country.get(country, 0) + 1
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'source': 'Marshall Valuation Service - Section 99',
                'updated': 'July 2025',
                'total_entries': total,
                'regional_entries': regional_count,
                'city_entries': city_count,
                'countries': by_country,
            },
            'multipliers': multipliers
        }, f, indent=2)
    
    print(f"[SAVED] JSON: {output_file}")
    print(f"   Total entries: {total} ({regional_count} regional, {city_count} cities)")
    print(f"   Countries: {', '.join(by_country.keys())}")


def main():
    """
    Main entry point for command-line usage
    """
    if len(sys.argv) < 4:
        print("Usage: python parse-local-multipliers.py <pdf_path> <start_page> <end_page>")
        print("Example: python parse-local-multipliers.py MVS.pdf 719 724")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    start_page = int(sys.argv[2])
    end_page = int(sys.argv[3])
    
    # Parse the PDF
    results = parse_local_multiplier_table(pdf_path, start_page, end_page)
    
    if results['success']:
        # Save output
        output_dir = Path(__file__).parent.parent / "Tables" / "Multipliers"
        
        # Save as markdown
        markdown_path = output_dir / "MULTIPLIER_LOCAL.md"
        save_to_markdown(results['multipliers'], str(markdown_path))
        
        # Save as JSON
        json_path = output_dir / "local_multipliers.json"
        save_to_json(results['multipliers'], str(json_path))
        
        print("\n[SUCCESS] Processing complete!")
    else:
        print("\n[ERROR] Processing failed!")
        for error in results['errors']:
            print(f"   - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
