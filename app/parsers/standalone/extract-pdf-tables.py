"""
Enhanced PDF Table Extractor for Marshall Valuation Service
Handles merged CLASS cells, header detection, and multiple tables per page
"""

import pdfplumber
import re
from pathlib import Path
from typing import List, Dict, Tuple

def get_text_characteristics(page, bbox) -> Dict:
    """
    Extract font characteristics from text within a bounding box
    Returns average font size and other characteristics
    """
    try:
        # Crop to the table area
        cropped = page.crop(bbox)
        chars = cropped.chars
        
        if not chars:
            return {'avg_size': 0, 'max_size': 0, 'has_bold': False, 'bold_ratio': 0}
        
        sizes = [c.get('size', 0) for c in chars if c.get('size')]
        avg_size = sum(sizes) / len(sizes) if sizes else 0
        max_size = max(sizes) if sizes else 0
        
        # Check for bold text (font name often contains 'Bold')
        bold_chars = sum(1 for c in chars if 'bold' in c.get('fontname', '').lower())
        bold_ratio = bold_chars / len(chars) if chars else 0
        
        return {
            'avg_size': avg_size,
            'max_size': max_size,
            'has_bold': bold_chars > 0,
            'bold_ratio': bold_ratio
        }
    except:
        return {'avg_size': 0, 'max_size': 0, 'has_bold': False, 'bold_ratio': 0}

def is_likely_header_row(first_row: List[str], text_chars: Dict, page=None, table_obj=None) -> bool:
    """
    Determine if the first row is likely a header based on content and formatting
    Analyzes the entire row, not just the first column
    """
    if not first_row:
        return False
    
    # Check for common header keywords (must be exact or very close matches)
    header_keywords = ['CLASS', 'TYPE', 'EXTERIOR', 'INTERIOR', 'LIGHTING', 
                       'PLUMBING', 'HEAT', 'COST', 'SQ.', 'CU.', 'FT.', 'M.']
    
    # Convert row to uppercase for comparison
    row_text = ' '.join([str(cell).upper() for cell in first_row if cell])
    
    # Count how many cells are actual header keywords (not just contain them)
    exact_header_cells = 0
    for cell in first_row:
        if not cell:
            continue
        cell_upper = str(cell).upper().strip()
        # Check if cell is a header keyword or contains multiple keywords
        if any(keyword == cell_upper or cell_upper.startswith(keyword) for keyword in header_keywords):
            exact_header_cells += 1
    
    # If row contains multiple header keywords, it's likely a header
    keyword_count = sum(1 for keyword in header_keywords if keyword in row_text)
    
    # Strong indicator: Multiple cells are header keywords (e.g., CLASS, TYPE, HEAT)
    if exact_header_cells >= 3:
        return True
    
    # Headers typically have 4+ keywords (stricter than before)
    if keyword_count >= 4:
        return True
    
    # NEW: Check formatting across the entire row
    # If we have access to the page and table object, analyze the whole row
    if page and table_obj:
        try:
            bbox = table_obj.bbox
            # Get the approximate height of one row
            table_height = bbox[3] - bbox[1]
            num_rows_estimate = max(2, len(first_row))  # Rough estimate
            row_height = min(30, table_height / num_rows_estimate)
            
            # Analyze the entire first row area
            first_row_bbox = (bbox[0], bbox[1], bbox[2], bbox[1] + row_height)
            full_row_chars = get_text_characteristics(page, first_row_bbox)
            
            # Headers typically have high bold ratio across the entire row (>50%)
            # Data rows might have bold in first column but not across the whole row
            if full_row_chars.get('bold_ratio', 0) > 0.5:
                # Most of the row is bold - likely a header
                return True
            elif full_row_chars.get('bold_ratio', 0) < 0.3 and keyword_count < 2:
                # Very little bold text and no header keywords - likely data
                return False
        except:
            pass
    
    # Don't rely solely on bold text in the first column - data rows can have bold first column
    return False

def fix_merged_cells(table_data: List[List[str]]) -> List[List[str]]:
    """
    Fix tables with merged cells in the first column (CLASS)
    Replicates the CLASS value down to child rows
    """
    if not table_data or len(table_data) < 2:
        return table_data
    
    fixed_table = []
    current_class = None
    
    for i, row in enumerate(table_data):
        if not row:
            continue
            
        # Skip header row
        if i == 0:
            fixed_table.append(row)
            continue
        
        # Check if first cell (CLASS) has a value
        if row[0] and str(row[0]).strip():
            # New CLASS value
            current_class = str(row[0]).strip()
            fixed_table.append(row)
        elif current_class and len(row) > 1 and any(row[1:]):
            # Empty CLASS but has data in other columns - fill with current class
            new_row = [current_class] + row[1:]
            fixed_table.append(new_row)
        else:
            # Row with no data - skip or keep as-is
            fixed_table.append(row)
    
    return fixed_table

def extract_table_title_from_position(page, table_obj, page_text: str) -> str:
    """
    Extract the title for a specific table by looking at text above it
    """
    if not table_obj:
        return extract_title_from_text(page_text)
    
    # Get the bounding box coordinates
    bbox = table_obj.bbox  # (x0, y0, x1, y1)
    table_top = bbox[1]  # y0 is the top
    
    # Extract text from above the table (200 points above to catch more context)
    crop_bbox = (0, max(0, table_top - 200), page.width, table_top)
    text_above = page.crop(crop_bbox).extract_text() or ""
    
    lines = [line.strip() for line in text_above.split('\n') if line.strip()]
    
    # Priority 1: Look for table-specific subtitle with occupancy code (closest to table)
    # Pattern: "CLASSES C AND D: SHELL LUXURY APARTMENT BUILDINGS (777)"
    for line in reversed(lines):  # Start from bottom (closest to table)
        # Skip common page elements
        if any(skip in line.upper() for skip in ['MARSHALL', 'CALCULATOR METHOD', 'SECTION ', 'PAGE ', 'NOTE:', 'ALTERNATE']):
            continue
        
        # Pattern 1: Subtitle with occupancy code (most specific)
        # e.g., "CLASSES C AND D: SHELL LUXURY APARTMENT BUILDINGS (777)"
        if re.search(r'\(\d{3,4}\)', line) and ':' in line:
            return line
        
        # Pattern 2: Any line with occupancy code
        if re.match(r'^[A-Z][A-Z\s\-–:&/()\d]+\(\d{3,4}\)$', line):
            return line
    
    # Priority 2: Look for section headings without codes but with descriptive format
    for line in reversed(lines):
        if any(skip in line.upper() for skip in ['MARSHALL', 'CALCULATOR', 'SECTION', 'PAGE', 'NOTE:', 'FOR']):
            continue
        
        # Pattern: "BASEMENTS - HIGH RISE APARTMENTS"
        if re.match(r'^[A-Z][A-Z\s\-–&/()\d]+$', line) and 15 < len(line) < 100:
            return line
    
    # Fallback to page-level title
    return extract_title_from_text(page_text)

def extract_section_and_page(text: str) -> Tuple[str, str]:
    """Extract section number and page number from header text"""
    # Look for "SECTION XX PAGE YY" pattern
    match = re.search(r'SECTION\s+(\d+)\s+PAGE\s+(\d+)', text, re.IGNORECASE)
    if match:
        return match.group(1), match.group(2)
    return "", ""

def extract_title_from_text(text: str) -> str:
    """Extract occupancy title from page text"""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    for line in lines:
        # Skip common headers
        if any(skip in line.upper() for skip in ['MARSHALL VALUATION', 'CALCULATOR METHOD', 'SECTION', 'PAGE']):
            continue
        
        # Look for title pattern with occupancy code
        if re.match(r'^[A-Z][A-Z\s\-–&/()\d]+\(\d+\)$', line):
            return line
        # Look for title pattern without code
        if re.match(r'^[A-Z][A-Z\s\-–&/()\d]+$', line) and 10 < len(line) < 100:
            return line
    
    return "Table"

def format_small_table(table_lines: list) -> str:
    """
    Format small deduction/adjustment tables into LLM-friendly text
    Converts "Good ..... 139.93 13.00" into "Good: 139.93 Sq. M. (13.00 Sq. Ft.)"
    """
    import re
    
    if not table_lines:
        return ""
    
    # Extract header to identify columns
    header = table_lines[0] if table_lines else ""
    has_sqm = 'Sq. M.' in header
    has_sqft = 'Sq. Ft.' in header
    
    formatted_rows = []
    
    for line in table_lines[1:]:  # Skip header
        # Extract quality level and numbers
        # Pattern: "Good ..... 139.93 13.00"
        
        # Find quality level at start
        quality_match = re.match(r'^(Good|Average|Excellent|Low cost|Low Cost)', line)
        if not quality_match:
            continue
        
        quality = quality_match.group(1)
        
        # Find all numbers in the line
        numbers = re.findall(r'\d+\.\d+', line)
        
        if len(numbers) >= 2:
            # Format as "Quality: XXX Sq. M. (YYY Sq. Ft.)"
            if has_sqm and has_sqft:
                formatted_rows.append(f"  {quality}: {numbers[0]} Sq. M. ({numbers[1]} Sq. Ft.)")
            elif has_sqm:
                formatted_rows.append(f"  {quality}: {numbers[0]} Sq. M.")
            elif has_sqft:
                formatted_rows.append(f"  {quality}: {numbers[0]} Sq. Ft.")
        elif len(numbers) == 1:
            formatted_rows.append(f"  {quality}: {numbers[0]}")
    
    if formatted_rows:
        # Add header if it contains useful info
        if 'CLASS' in header.upper():
            result = header + '\n' + '\n'.join(formatted_rows)
        else:
            result = '\n'.join(formatted_rows)
        return result
    
    return ""

def extract_table_notes(page, table_obj, page_text: str) -> str:
    """
    Extract notes and methodology text below a table
    This captures building-specific multiplier instructions
    Uses word-level extraction to handle multi-column layouts properly
    Stops at the next section header to avoid including subsequent tables
    """
    if not table_obj:
        return ""
    
    try:
        bbox = table_obj.bbox
        table_bottom = bbox[3]  # y1 is the bottom of table
        
        # Find the next section header to determine where notes end
        # Look for patterns like "BUILD-OUT (997)" or "INTERIOR BUILD-OUT"
        all_words = page.extract_words(keep_blank_chars=True, x_tolerance=3, y_tolerance=3)
        
        notes_end = page.height  # Default to end of page
        for i, word in enumerate(all_words):
            text = word['text'].strip()
            y = word['top']
            
            # Must be below the table
            if y > table_bottom + 50:
                # Pattern 1: "BUILD-OUT" followed by "(997)" or similar
                if text.upper() == 'BUILD-OUT':
                    # Check next word for parentheses pattern
                    if i + 1 < len(all_words):
                        next_word = all_words[i + 1]['text'].strip()
                        if '(' in next_word and ')' in next_word:
                            notes_end = y
                            print(f"      Found next section at y={y:.1f}: {text} {next_word}")
                            break
                
                # Pattern 2: Long section headers with "INTERIOR" or "BUILD-OUT"
                if len(text) > 20 and ('INTERIOR' in text.upper() or 'BUILD-OUT' in text.upper()):
                    notes_end = y
                    print(f"      Found next section at y={y:.1f}: {text[:50]}...")
                    break
        
        # Crop to area below table, stopping at next section
        crop_bbox = (0, table_bottom + 5, page.width, notes_end - 5)
        cropped_page = page.crop(crop_bbox)
        
        # Extract words with positions to handle columns
        words = cropped_page.extract_words(
            x_tolerance=3,
            y_tolerance=3,
            keep_blank_chars=False,
        )
        
        if not words:
            return ""
        
        # Detect if we have a multi-column layout
        # Group words by Y position to find lines
        page_mid_x = page.width / 2
        
        # Group words into lines based on Y coordinate
        from collections import defaultdict
        lines_dict = defaultdict(list)
        
        for word in words:
            # Round y position to group into lines (tolerance of 5 pixels for better line grouping)
            line_y = round(word['top'] / 5) * 5
            lines_dict[line_y].append(word)
        
        # Process columns separately to preserve all content
        # Group into left and right columns, maintaining line order within each
        left_column_lines = []
        right_column_lines = []
        
        for y in sorted(lines_dict.keys()):
            line_words = sorted(lines_dict[y], key=lambda w: w['x0'])
            
            # Separate into columns
            left_words = [w for w in line_words if w['x0'] < page_mid_x]
            right_words = [w for w in line_words if w['x0'] >= page_mid_x]
            
            # Add to respective columns
            if left_words:
                left_text = ' '.join(w['text'] for w in left_words).strip()
                if left_text:
                    left_column_lines.append(left_text)
            
            if right_words:
                right_text = ' '.join(w['text'] for w in right_words).strip()
                if right_text:
                    right_column_lines.append(right_text)
        
        # Combine: all left column content first, then blank line, then right column
        all_lines = left_column_lines + [''] + right_column_lines
        
        # Clean up the notes text
        cleaned_lines = []
        
        for line in all_lines:
            # Skip empty lines
            if not line:
                continue
            
            # Skip common footers and headers
            # Note: Be specific to avoid filtering legitimate content
            skip_patterns = [
                'MARSHALL VALUATION',
                'CALCULATOR METHOD',
                # Don't use 'SECTION ' as it filters lines like "from Section 12"
                # Only filter if it's a page header format
                'SECTION 11 PAGE',  # Specific page header format
                '© ',
                'CORECOST',
                'PROHIBITED',
                'OBSOLETE AFTER UPDATE',
                '/2024',
                '/2025',
                '/2026',
            ]
            
            if any(pattern in line.upper() for pattern in skip_patterns):
                continue
            
            # Skip page numbers (standalone numbers)
            if line.strip().isdigit():
                continue
            
            # Skip lines that are mostly dots (table formatting artifact)
            # But allow lines with dots if they have substantial text too
            if line.count('.') > len(line) * 0.7:
                continue
            
            cleaned_lines.append(line)
        
        # Smart paragraph joining: only break paragraphs at section headers or complete sentences
        # Detect and format small tables nicely for LLM consumption
        paragraphs = []
        current_paragraph = []
        in_table = False
        table_lines = []
        
        for i, line in enumerate(cleaned_lines):
            # Check if this line is tabular data (row with quality level + numbers)
            # Look for patterns like "Good ..... 139.93 13.00"
            is_table_row = (
                any(qual in line for qual in ['Good', 'Average', 'Excellent', 'Low cost', 'Low Cost']) and
                line.count('.') >= 3 and  # Has dotted leaders
                any(char.isdigit() for char in line)
            )
            
            # Check if this is a table header like "CLASSES A and B Sq. M. Sq. Ft."
            is_table_header = (
                ('Sq. M.' in line and 'Sq. Ft.' in line) and
                ('CLASS' in line.upper() or line.isupper())
            )
            
            # Check if this line starts a new section
            is_new_section = (
                line.startswith('NOTE:') or
                line.startswith('*') or
                (len(line) < 80 and line.isupper() and '–' in line) or
                (i > 0 and len(line) > 10 and line[0].isupper() and cleaned_lines[i-1].endswith('.'))
            )
            
            if is_table_header:
                # Save current paragraph, start collecting table
                if current_paragraph:
                    paragraphs.append(' '.join(current_paragraph))
                    current_paragraph = []
                in_table = True
                table_lines = [line]
            elif is_table_row and in_table:
                # Collect table rows
                table_lines.append(line)
            elif in_table:
                # End of table, format it nicely
                formatted_table = format_small_table(table_lines)
                if formatted_table:
                    paragraphs.append(formatted_table)
                in_table = False
                table_lines = []
                # Process this line normally
                if is_new_section and current_paragraph:
                    paragraphs.append(' '.join(current_paragraph))
                    current_paragraph = [line]
                else:
                    current_paragraph.append(line)
            elif is_new_section and current_paragraph:
                # Save current paragraph and start new one
                paragraphs.append(' '.join(current_paragraph))
                current_paragraph = [line]
            else:
                # Continue current paragraph
                current_paragraph.append(line)
        
        # Handle any remaining table
        if in_table and table_lines:
            formatted_table = format_small_table(table_lines)
            if formatted_table:
                paragraphs.append(formatted_table)
        
        # Add final paragraph
        if current_paragraph:
            paragraphs.append(' '.join(current_paragraph))
        
        # Join paragraphs with double line breaks
        notes = '\n\n'.join(paragraphs).strip()
        
        # Only return if we have substantial content
        if len(notes) > 50:
            return notes
        
        return ""
        
    except Exception as e:
        print(f"    [!] Error extracting notes: {e}")
        return ""

def clean_table_title(title: str) -> str:
    """Clean up table title for filename"""
    # Remove occupancy code in parentheses
    clean = re.sub(r'\s*\(\d+\)\s*$', '', title)
    # Replace special chars with underscores
    clean = re.sub(r'[^A-Za-z0-9]+', '_', clean)
    # Clean up multiple underscores
    clean = re.sub(r'_+', '_', clean).strip('_')
    return clean

def is_footnote_row(row: List[str]) -> bool:
    """
    Detect if a row is a footnote/note row rather than actual table data.
    These are rows where text spans across most columns and contains explanatory content.
    """
    if not row:
        return False
    
    # Get the first non-empty cell
    first_cell = None
    for cell in row:
        if cell and str(cell).strip():
            first_cell = str(cell).strip()
            break
    
    if not first_cell:
        return False
    
    # Check for footnote indicators
    # Pattern 1: Starts with * (footnote marker)
    if first_cell.startswith('*'):
        return True
    
    # Pattern 2: Long text that looks like a note/explanation
    # These typically have most columns empty and one cell with lots of text
    non_empty_cells = [c for c in row if c and str(c).strip()]
    if len(non_empty_cells) <= 2:  # Only 1-2 cells have content
        total_text = ' '.join(str(c) for c in non_empty_cells)
        # Long explanatory text (>100 chars) with note-like patterns
        if len(total_text) > 100:
            note_patterns = [
                'COMPLETE HEATING',
                'VENTILATING AND AIR',
                'Because of the',
                'For further refinement',
                'see bottom of',
                'see Page',
                'NOTE:',
                'NOTES:',
            ]
            if any(pattern in total_text.upper() for pattern in note_patterns):
                return True
    
    return False


def separate_table_and_footnotes(table_data: List[List[str]]) -> tuple:
    """
    Separate actual table rows from footnote rows.
    Returns (clean_table_data, footnote_texts)
    """
    if not table_data:
        return [], []
    
    clean_rows = [table_data[0]]  # Always keep header
    footnotes = []
    
    for row in table_data[1:]:
        if is_footnote_row(row):
            # Extract text from this footnote row
            text = ' '.join(str(c).strip() for c in row if c and str(c).strip())
            text = ' '.join(text.split())  # Collapse whitespace
            if text:
                footnotes.append(text)
        else:
            clean_rows.append(row)
    
    return clean_rows, footnotes


def format_table_to_markdown(table_data: List[List[str]]) -> tuple:
    """
    Convert table to clean markdown with proper structure.
    Returns (markdown_string, footnote_texts)
    """
    if not table_data or len(table_data) < 2:
        return "", []
    
    # Separate table data from footnotes
    clean_table, footnotes = separate_table_and_footnotes(table_data)
    
    if len(clean_table) < 2:
        return "", footnotes
    
    # Build markdown - just the table header
    md = "## Cost Table\n\n"
    
    # Headers
    headers = clean_table[0]
    # Clean headers - remove line breaks, collapse spaces
    clean_headers = []
    for h in headers:
        if h:
            h_str = str(h).replace('\n', ' ').replace('\r', ' ').strip()
            h_str = ' '.join(h_str.split())  # Collapse multiple spaces
            clean_headers.append(h_str)
        else:
            clean_headers.append("")
    
    md += "| " + " | ".join(clean_headers) + " |\n"
    md += "| " + " | ".join(["---" for _ in clean_headers]) + " |\n"
    
    # Data rows - clean up cells
    for row in clean_table[1:]:
        if not row or not any(row):
            continue
        
        clean_row = []
        for cell in row:
            if cell:
                # Remove line breaks and extra spaces from cells
                cell_str = str(cell).replace('\n', ' ').replace('\r', ' ').strip()
                cell_str = ' '.join(cell_str.split())
                clean_row.append(cell_str)
            else:
                clean_row.append("")
        
        # Only add row if it has substantial data
        if any(clean_row):
            md += "| " + " | ".join(clean_row) + " |\n"
    
    return md, footnotes

def process_pdf_pages(pdf_path: str, output_dir: str, start_page: int = 1, end_page: int = None):
    """Process PDF pages with enhanced table handling (header detection, merged cells)"""
    pdf_path = Path(pdf_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print(f"Opening PDF: {pdf_path}")
    
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        if end_page is None:
            end_page = total_pages
        
        print(f"Processing PDF pages {start_page} to {end_page} (of {total_pages} total)")
        
        for pdf_page_num in range(start_page, end_page + 1):
            if pdf_page_num > len(pdf.pages):
                break
            
            page = pdf.pages[pdf_page_num - 1]
            print(f"\nPDF Page {pdf_page_num}...")
            
            # Extract page text for context
            page_text = page.extract_text() or ""
            
            # Extract section and page number from header
            section_num, section_page = extract_section_and_page(page_text)
            if not section_num or not section_page:
                print(f"  Could not extract section/page from header")
                continue
            
            # Extract tables with bounding boxes
            raw_tables = page.extract_tables({
                'vertical_strategy': 'lines',
                'horizontal_strategy': 'lines',
                'snap_tolerance': 3,
                'join_tolerance': 3,
            })
            
            # Also get table bounding boxes for position-based title extraction
            table_settings = {
                'vertical_strategy': 'lines',
                'horizontal_strategy': 'lines',
                'snap_tolerance': 3,
                'join_tolerance': 3,
            }
            table_finder = page.find_tables(table_settings)
            
            if not raw_tables:
                print(f"  No tables found")
                continue
            
            print(f"  Found {len(raw_tables)} table(s)")
            
            # Store headers from first valid table to propagate to subsequent tables
            page_headers = None
            
            # Process each table separately and create individual files
            for table_idx, raw_table in enumerate(raw_tables):
                # Skip empty tables, but allow tables with at least 2 rows (header + 1 data row)
                if not raw_table or len(raw_table) < 2:
                    continue
                
                # Get table object for position-based title extraction
                table_obj = table_finder[table_idx] if table_idx < len(table_finder) else None
                
                # Check if this table has valid headers
                first_row = raw_table[0] if raw_table else []
                
                # Get text characteristics for header detection
                text_chars = {}
                if table_obj:
                    bbox = table_obj.bbox
                    # Get characteristics of just the first row
                    first_row_bbox = (bbox[0], bbox[1], bbox[2], bbox[1] + 30)  # Approximate first row height
                    text_chars = get_text_characteristics(page, first_row_bbox)
                
                # Pass page and table_obj for enhanced header detection
                has_headers = is_likely_header_row(first_row, text_chars, page, table_obj)
                
                # Store headers from first table with valid headers
                if table_idx == 0 and has_headers:
                    page_headers = first_row
                    print(f"  Table {table_idx + 1}: Has headers - storing for page")
                elif not has_headers and page_headers:
                    # This table is missing headers - insert the stored ones
                    print(f"  Table {table_idx + 1}: Missing headers - using headers from first table")
                    raw_table = [page_headers] + raw_table
                else:
                    print(f"  Table {table_idx + 1}: Processing normally")
                
                # Extract title specific to this table's position
                table_title = extract_table_title_from_position(page, table_obj, page_text)
                
                # Extract occupancy code if present
                occupancy_code = ""
                match = re.search(r'\((\d+)\)', table_title)
                if match:
                    occupancy_code = match.group(1)
                
                # Clean title for filename
                clean_title = clean_table_title(table_title)
                
                # Generate filename
                filename = f"S{section_num}_P{section_page}_{clean_title}.md"
                filepath = output_dir / filename
                
                # Overwrite existing files (no duplicate counter)
                
                # Fix merged cells in CLASS column
                fixed_table = fix_merged_cells(raw_table)
                
                # Extract notes/methodology from below the table
                table_notes = extract_table_notes(page, table_obj, page_text)
                
                # Build markdown content for this table
                content = f"# {table_title}\n\n"
                if occupancy_code:
                    content += f"**Occupancy Code:** {occupancy_code}\n"
                content += f"**Source:** Marshall Valuation Service, Section {section_num}, Page {section_page}\n"
                content += f"**PDF Page:** {pdf_page_num}\n\n"
                
                # Add table (now returns tuple with footnotes)
                table_md, table_footnotes = format_table_to_markdown(fixed_table)
                content += table_md
                
                # Add notes section if we have footnotes or extracted notes
                if table_footnotes or table_notes:
                    content += "\n## Notes & Methodology\n\n"
                    
                    # Add footnotes extracted from the table first
                    if table_footnotes:
                        for footnote in table_footnotes:
                            content += footnote + "\n\n"
                    
                    # Add notes extracted from below the table
                    if table_notes:
                        content += table_notes + "\n"
                    
                    total_notes = len(table_notes) + sum(len(f) for f in table_footnotes)
                    print(f"    [+] Extracted {total_notes} chars of notes ({len(table_footnotes)} footnotes)")
                
                # Save file
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"  ✓ Saved: {filename}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python extract-pdf-tables.py <path-to-mvs.pdf> [output-directory] [start-page] [end-page]")
        print("\nExamples:")
        print("  python extract-pdf-tables.py ../data/pdfs/MVS.pdf ../Tables")
        print("  python extract-pdf-tables.py ../data/pdfs/MVS.pdf ../Tables 67 92")
        print("  python extract-pdf-tables.py ../data/pdfs/MVS.pdf ../Tables/Section11 67 92")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "../Tables"
    start_page = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    end_page = int(sys.argv[4]) if len(sys.argv) > 4 else None
    
    print("=" * 60)
    print("MVS Enhanced PDF Table Extractor")
    print("=" * 60)
    
    process_pdf_pages(pdf_path, output_dir, start_page, end_page)
    
    print("\n" + "=" * 60)
    print("Extraction complete!")
    print("=" * 60)
