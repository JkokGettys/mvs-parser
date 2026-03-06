"""
Parse S13 Floor Area/Perimeter Multiplier tables from markdown files.
Section 13 has 3 sub-tables across 2 pages (S13P41 small buildings, S13P41 medium buildings, S13P42 large buildings).
All share the same input/output structure: floor_area_sqft x perimeter_ft -> multiplier.
Output: S13_floor_area_perimeter.json matching S11 format.
"""

import json
import re
import os

def parse_markdown_table(filepath):
    """Parse a markdown FA/P table into (floor_area, perimeter, multiplier) tuples."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    lines = content.split('\n')
    
    # Find the cost table section
    # Note: data rows contain '-----' which includes '---' as substring
    # Separator rows look like "| --- | --- | --- |" - detect by checking if ALL cells are separators
    table_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith('|'):
            continue
        cells = [c.strip() for c in stripped.split('|') if c.strip()]
        # A separator row has ALL cells matching the pattern '---' (dashes only)
        is_separator = all(c == '---' or (len(c) >= 3 and all(ch == '-' for ch in c) and len(c) <= 5) for c in cells)
        if not is_separator:
            table_lines.append(stripped)
    
    if len(table_lines) < 4:
        print(f"  Warning: Not enough table lines in {filepath}")
        return []
    
    # Use raw split (keeping empty strings from ||) to preserve column positions
    # Line 2 (index 2) has the FT row: | Sq.M. | Sq. Ft. | FT. | 50 | 75 | ... |
    ft_header = table_lines[2]
    ft_cells = [c.strip() for c in ft_header.split('|')]
    # ft_cells[0] is '' (before first |), ft_cells[-1] is '' (after last |)
    
    # Find the FT. markers to locate perimeter columns
    ft_indices = [i for i, c in enumerate(ft_cells) if c == 'FT.']
    if len(ft_indices) < 2:
        print(f"  Warning: Could not find FT. column markers in {filepath}")
        return []
    
    data_start_col = ft_indices[0] + 1  # first perimeter value column
    data_end_col = ft_indices[1]        # exclusive end
    
    perimeter_values = []
    for i in range(data_start_col, data_end_col):
        try:
            val = int(ft_cells[i].replace(',', ''))
            perimeter_values.append(val)
        except ValueError:
            perimeter_values.append(None)
    
    print(f"  Perimeter values ({len(perimeter_values)}): {[p for p in perimeter_values if p]}")
    print(f"  Data columns: {data_start_col} to {data_end_col}")
    
    # Parse data rows (starting from line 3)
    results = []
    for i in range(3, len(table_lines)):
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
        
        # Data cells at same column positions as header
        for j, pval in enumerate(perimeter_values):
            if pval is None:
                continue
            col_idx = data_start_col + j
            if col_idx >= len(cells):
                break
            
            cell_val = cells[col_idx].strip()
            if cell_val in ('-----', '', '-', '----'):
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
    section_dir = os.path.join(base_dir, 'Tables', 'Section 13')
    output_dir = os.path.join(base_dir, 'Tables', 'Refinements', 'FloorAreaPerimeter')
    
    all_multipliers = []
    
    # Table 1: S13_P41_FLOOR_AREA_PERIMETER_MULTIPLIERS.md (small buildings: 500-45,000 sqft)
    file1 = os.path.join(section_dir, 'S13_P41_FLOOR_AREA_PERIMETER_MULTIPLIERS.md')
    print(f"Parsing Table 1 (small): {os.path.basename(file1)}")
    results1 = parse_markdown_table(file1)
    print(f"  Got {len(results1)} entries")
    all_multipliers.extend(results1)
    
    # Table 2: S13_P41_STORES_AND_COMMERCIAL_BUILDINGS.md (medium: 9,000-150,000 sqft)
    file2 = os.path.join(section_dir, 'S13_P41_STORES_AND_COMMERCIAL_BUILDINGS.md')
    print(f"\nParsing Table 2 (medium): {os.path.basename(file2)}")
    results2 = parse_markdown_table(file2)
    print(f"  Got {len(results2)} entries")
    all_multipliers.extend(results2)
    
    # Table 3: S13_P42_STORES_AND_COMMERCIAL_BUILDINGS.md (large: 200,000-500,000 sqft)
    file3 = os.path.join(section_dir, 'S13_P42_STORES_AND_COMMERCIAL_BUILDINGS.md')
    print(f"\nParsing Table 3 (large): {os.path.basename(file3)}")
    results3 = parse_markdown_table(file3)
    print(f"  Got {len(results3)} entries")
    all_multipliers.extend(results3)
    
    # Deduplicate (some rows may overlap between tables)
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
            "source": "Marshall Valuation Service - Section 13, Pages 41-42",
            "pdf_page": 175,
            "description": "Floor area/perimeter multipliers for Stores & Commercial Buildings - adjustments based on building size and shape",
            "notes": "For larger centers, enter table with half the average floor area and half the average perimeter."
        },
        "perimeter_values_ft": perimeter_set,
        "floor_area_values_sqft": floor_area_set,
        "multipliers": unique
    }
    
    output_path = os.path.join(output_dir, 'S13_floor_area_perimeter.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nTotal unique entries: {len(unique)}")
    print(f"Floor area range: {floor_area_set[0]:,} - {floor_area_set[-1]:,} sqft")
    print(f"Perimeter range: {perimeter_set[0]:,} - {perimeter_set[-1]:,} ft")
    print(f"Output: {output_path}")


if __name__ == '__main__':
    main()
