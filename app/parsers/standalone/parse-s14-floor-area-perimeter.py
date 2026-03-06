"""
Parse S14 Floor Area/Perimeter Multiplier tables from markdown files.
Section 14 has 2 sub-tables across 2 pages:
  - S14_P38 has TWO sub-tables (small buildings 1k-50k sqft, medium 20k-500k sqft)
  - S14_P39 has ONE sub-table (large buildings 300k-1,500k sqft)
All share the same input/output structure: floor_area_sqft x perimeter_ft -> multiplier.
Output: S14_floor_area_perimeter.json matching S13/S11 format.
"""

import json
import re
import os


def parse_markdown_table(filepath, sub_table_index=0):
    """Parse a markdown FA/P table into (floor_area, perimeter, multiplier) tuples.
    
    sub_table_index: which sub-table to parse (0-based). S14_P38 has two sub-tables
    separated by a second header block within the same file.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    lines = content.split('\n')
    
    # Filter to only pipe-delimited lines, skip separator rows
    table_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith('|'):
            continue
        cells = [c.strip() for c in stripped.split('|') if c.strip()]
        # A separator row has ALL cells matching dashes only
        is_separator = all(len(c) >= 3 and all(ch == '-' for ch in c) for c in cells)
        if not is_separator:
            table_lines.append(stripped)
    
    if len(table_lines) < 4:
        print(f"  Warning: Not enough table lines in {filepath}")
        return []
    
    # Split into sub-tables by finding FT. header rows
    # Each sub-table starts with AVERAGE/FLOOR AREA/Sq.M. header trio
    sub_tables = []
    current_start = 0
    ft_row_indices = []
    
    for i, line in enumerate(table_lines):
        cells = [c.strip() for c in line.split('|')]
        if 'FT.' in cells and 'Sq. Ft.' in cells:
            ft_row_indices.append(i)
    
    if sub_table_index >= len(ft_row_indices):
        print(f"  Warning: sub_table_index {sub_table_index} but only {len(ft_row_indices)} FT rows found")
        return []
    
    # Determine the range of lines for this sub-table
    ft_idx = ft_row_indices[sub_table_index]
    # Data starts right after the FT. row
    data_start = ft_idx + 1
    # Data ends at the next header block or end of file
    if sub_table_index + 1 < len(ft_row_indices):
        # Next sub-table's FT row is at ft_row_indices[sub_table_index+1]
        # The header block before it has 2 rows (AVERAGE, FLOOR AREA, then FT.)
        data_end = ft_row_indices[sub_table_index + 1] - 2
    else:
        data_end = len(table_lines)
    
    # Parse the FT. header row for perimeter values
    ft_header = table_lines[ft_idx]
    ft_cells = [c.strip() for c in ft_header.split('|')]
    
    # Find FT. markers
    ft_positions = [i for i, c in enumerate(ft_cells) if c == 'FT.']
    if len(ft_positions) < 2:
        print(f"  Warning: Could not find 2 FT. column markers")
        return []
    
    data_start_col = ft_positions[0] + 1
    data_end_col = ft_positions[1]
    
    perimeter_values = []
    for i in range(data_start_col, data_end_col):
        try:
            val = int(ft_cells[i].replace(',', ''))
            perimeter_values.append(val)
        except (ValueError, IndexError):
            perimeter_values.append(None)
    
    print(f"  Perimeter values ({len([p for p in perimeter_values if p])}): {[p for p in perimeter_values if p]}")
    
    # Parse data rows
    results = []
    for i in range(data_start, data_end):
        if i >= len(table_lines):
            break
        row = table_lines[i]
        cells = [c.strip() for c in row.split('|')]
        
        if len(cells) < data_end_col:
            continue
        
        # cells[1] = Sq.M., cells[2] = Sq.Ft. (floor area)
        sqft_str = cells[2].replace(',', '').strip()
        try:
            floor_area_sqft = int(sqft_str)
        except ValueError:
            continue
        
        for j, pval in enumerate(perimeter_values):
            if pval is None:
                continue
            col_idx = data_start_col + j
            if col_idx >= len(cells):
                break
            
            cell_val = cells[col_idx].strip()
            if cell_val in ('-----', '------', '', '-', '----'):
                continue
            
            try:
                multiplier = float(cell_val)
                results.append({
                    'floor_area_sqft': floor_area_sqft,
                    'perimeter_ft': pval,
                    'multiplier': multiplier
                })
            except ValueError:
                continue
    
    return results


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    section_dir = os.path.join(base_dir, 'Tables', 'Section 14')
    output_dir = os.path.join(base_dir, 'Tables', 'Refinements', 'FloorAreaPerimeter')
    
    all_multipliers = []
    
    # Table 1: S14_P38 sub-table 1 (small buildings: 1,000-50,000 sqft, perimeters 100-1000)
    file1 = os.path.join(section_dir, 'S14_P38_FLOOR_AREA_PERIMETER_MULTIPLIERS.md')
    print(f"Parsing Table 1 (small, P38 sub-table 0): {os.path.basename(file1)}")
    results1 = parse_markdown_table(file1, sub_table_index=0)
    print(f"  Got {len(results1)} entries")
    all_multipliers.extend(results1)
    
    # Table 2: S14_P38 sub-table 2 (medium buildings: 20,000-500,000 sqft, perimeters 900-3000)
    print(f"\nParsing Table 2 (medium, P38 sub-table 1): {os.path.basename(file1)}")
    results2 = parse_markdown_table(file1, sub_table_index=1)
    print(f"  Got {len(results2)} entries")
    all_multipliers.extend(results2)
    
    # Table 3: S14_P39 (large buildings: 300,000-1,500,000 sqft, perimeters 2000-8000)
    file3 = os.path.join(section_dir, 'S14_P39_FLOOR_AREA_PERIMETER_MULTIPLIERS.md')
    print(f"\nParsing Table 3 (large, P39): {os.path.basename(file3)}")
    results3 = parse_markdown_table(file3, sub_table_index=0)
    print(f"  Got {len(results3)} entries")
    all_multipliers.extend(results3)
    
    # Deduplicate (overlapping rows between sub-tables)
    seen = set()
    unique = []
    for m in all_multipliers:
        key = (m['floor_area_sqft'], m['perimeter_ft'])
        if key not in seen:
            seen.add(key)
            unique.append(m)
    
    # Sort by floor area then perimeter
    unique.sort(key=lambda x: (x['floor_area_sqft'], x['perimeter_ft']))
    
    # Collect all unique perimeter and floor area values
    perimeter_set = sorted(set(m['perimeter_ft'] for m in unique))
    floor_area_set = sorted(set(m['floor_area_sqft'] for m in unique))
    
    output = {
        "metadata": {
            "source": "Marshall Valuation Service - Section 14, Pages 38-39",
            "pdf_page": 214,
            "description": "Floor area/perimeter multipliers for Garages, Industrials, Lofts & Warehouses - adjustments based on building size and shape",
            "notes": "For larger buildings, enter the table by taking half the area and half the perimeter."
        },
        "perimeter_values_ft": perimeter_set,
        "floor_area_values_sqft": floor_area_set,
        "multipliers": unique
    }
    
    output_path = os.path.join(output_dir, 'S14_floor_area_perimeter.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nTotal unique entries: {len(unique)}")
    print(f"Floor area range: {floor_area_set[0]:,} - {floor_area_set[-1]:,} sqft")
    print(f"Perimeter range: {perimeter_set[0]:,} - {perimeter_set[-1]:,} ft")
    print(f"Output: {output_path}")


if __name__ == '__main__':
    main()
