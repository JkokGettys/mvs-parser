#!/usr/bin/env python3
"""
Elevator and Escalator Table Parser for MVS PDF
Extracts elevator tables from Section 58 of the MVS PDF and outputs structured JSON
"""

import re
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import pdfplumber

def clean_number(text: str) -> Optional[int]:
    """Clean and convert text to integer, handling ranges and formatting
    Handles values like '44200.00' as $44,200 (integer dollars)
    """
    if not text or text.strip() in ['', '-----', '----', '---']:
        return None
    
    # Remove whitespace, commas, and dollar signs but KEEP decimal point
    cleaned = re.sub(r'[\s,$]', '', text.strip())
    
    # Handle ranges (take the midpoint) - handle both en-dash and hyphen
    if '–' in cleaned or '—' in cleaned or '-' in cleaned:
        # Split on any type of dash
        parts = re.split(r'[–—-]', cleaned)
        if len(parts) == 2 and parts[0] and parts[1]:
            try:
                # Convert to float first to handle decimal notation, then to int
                val1 = int(float(parts[0]))
                val2 = int(float(parts[1]))
                return (val1 + val2) // 2
            except ValueError:
                pass
    
    # Try to convert single value (float first, then int)
    try:
        return int(float(cleaned))
    except ValueError:
        return None

def extract_passenger_elevator_table(lines: List[str], table_name: str) -> Dict[str, Any]:
    """
    Extract passenger elevator table (Speed × Capacity layout)
    Returns: {speeds: [], capacities: [], costs: [[]], cost_per_stop: {}}
    """
    speeds = []
    capacities = []
    cost_matrix = []
    cost_per_stop = {}
    
    # Find capacity header line
    capacity_line_idx = -1
    for idx, line in enumerate(lines):
        # Look for line with "CAPACITY" header
        if 'CAPACITY' in line.upper() and 'POUNDS' in line.upper():
            # Look for capacity values in the next few lines
            # Skip lines with descriptive text (containing words like "cost", "control", etc.)
            for offset in range(1, min(4, len(lines) - idx)):
                test_line = lines[idx + offset]
                
                # Skip lines that look like descriptive text
                if any(word in test_line.lower() for word in ['cost', 'control', 'cab', 'stop', 'elevator']):
                    continue
                
                # Look for lines with multiple 4-digit numbers
                cap_numbers = re.findall(r'\b(\d{4,5})\b', test_line)
                valid_capacities = [int(c) for c in cap_numbers if 1000 <= int(c) <= 20000]
                
                # Need at least 2 capacity values to be a valid table
                if len(valid_capacities) >= 2:
                    capacities = valid_capacities
                    capacity_line_idx = idx + offset
                    break
            
            if capacities:
                break
    
    if capacity_line_idx == -1 or not capacities:
        print(f"Warning: Could not find capacity header for {table_name}")
        return None
    
    # Extract speed and cost rows
    found_cost_per_stop = False
    for i in range(capacity_line_idx + 1, len(lines)):
        line = lines[i]
        
        # Pattern 1: "PLUS" on its own line
        if line.strip().upper() == 'PLUS' and not found_cost_per_stop:
            # Next line should have the cost values
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                cost_values = re.findall(r'\d+\.?\d*\s*[–—-]\s*\d+\.?\d*', next_line)
                
                if cost_values:
                    for idx, val_str in enumerate(cost_values[:len(capacities)]):
                        cost = clean_number(val_str)
                        if cost and idx < len(capacities):
                            cost_per_stop[capacities[idx]] = cost
                    found_cost_per_stop = True
                    # Skip the next line (COST PER STOP label)
                    continue
        
        # Pattern 2: "PLUS" and "COST PER STOP" on same line with values
        if 'PLUS' in line.upper() and 'COST PER STOP' in line.upper() and not found_cost_per_stop:
            cost_values = re.findall(r'\d+\.?\d*\s*[–—-]\s*\d+\.?\d*', line)
            
            if cost_values:
                for idx, val_str in enumerate(cost_values[:len(capacities)]):
                    cost = clean_number(val_str)
                    if cost and idx < len(capacities):
                        cost_per_stop[capacities[idx]] = cost
                found_cost_per_stop = True
            break
        
        # Extract speed and costs (before PLUS line)
        if not found_cost_per_stop:
            # Look for lines starting with a number (speed) followed by cost ranges
            speed_match = re.match(r'^(\d+)\s+(.+)', line)
            if speed_match:
                try:
                    speed = int(speed_match.group(1))
                    # Only accept valid speeds (typically 50-1500 FPM)
                    if 25 <= speed <= 1500:
                        speeds.append(speed)
                        
                        # Extract cost ranges for each capacity
                        rest_of_line = speed_match.group(2)
                        cost_ranges = re.findall(r'\d+\.?\d*\s*[–—-]\s*\d+\.?\d*', rest_of_line)
                        
                        row_costs = []
                        for val_str in cost_ranges[:len(capacities)]:
                            cost = clean_number(val_str)
                            row_costs.append(cost)
                        
                        # Pad if needed
                        while len(row_costs) < len(capacities):
                            row_costs.append(None)
                        
                        cost_matrix.append(row_costs[:len(capacities)])
                except ValueError:
                    pass
    
    if not speeds or not capacities:
        return None
    
    return {
        "type": "passenger",
        "name": table_name,
        "speeds": speeds,
        "capacities": capacities,
        "costs": cost_matrix,
        "cost_per_stop": cost_per_stop
    }

def extract_freight_elevator_table(lines: List[str], table_name: str) -> Dict[str, Any]:
    """
    Extract freight elevator table (Capacity × Speed layout)
    Returns: {capacities: [], speeds: [], costs: [[]], cost_per_stop_manual: {}, cost_per_stop_power: {}}
    """
    capacities = []
    speeds = []
    cost_matrix = []
    cost_per_stop_manual = {}
    cost_per_stop_power = {}
    
    # Find speed header line and extract speeds
    # For freight elevators, the format is:
    # Line 1: "CAPACITY SPEED (Feet per Minute) PLUS COST PER STOP"
    # Line 2: "(Pounds)"
    # Line 3: "50 100 125 150 Manual Doors Power Doors"
    
    speed_line_idx = -1
    for idx, line in enumerate(lines):
        if 'SPEED' in line.upper() and 'MINUTE' in line.upper() and 'PLUS COST PER STOP' in line.upper():
            speed_line_idx = idx
            # Look for speeds in the next few lines
            for offset in range(1, min(4, len(lines) - idx)):
                next_line = lines[idx + offset]
                # Look for a line that starts with numbers (speeds)
                # And possibly ends with "Manual Doors" and "Power Doors"
                speed_numbers = re.findall(r'\b(\d+)\b', next_line.split('Manual')[0] if 'Manual' in next_line else next_line)
                speeds = [int(s) for s in speed_numbers if 25 <= int(s) <= 500]
                if speeds:
                    break
            break
    
    if speed_line_idx == -1 or not speeds:
        print(f"Warning: Could not find speed header for {table_name}")
        return None
    
    # Look for the header line that says "Manual Doors" and "Power Doors"
    door_type_line_idx = -1
    for idx in range(speed_line_idx, min(speed_line_idx + 3, len(lines))):
        if 'MANUAL' in lines[idx].upper() and 'POWER' in lines[idx].upper():
            door_type_line_idx = idx
            break
    
    # Extract capacity and cost rows
    for i in range(speed_line_idx + 1, len(lines)):
        line = lines[i]
        
        # Stop if we hit another section
        if 'ELECTRIC' in line.upper() or 'REAR DOOR' in line.upper() or 'SELECTIVE' in line.upper():
            break
        
        # Check if this line contains capacity (starts with number like 2,000 or 2000)
        cap_match = re.match(r'^(\d[\d,]*)\s+(.+)', line)
        if cap_match:
            cap_str = cap_match.group(1).replace(',', '').strip()
            try:
                capacity = int(cap_str)
                if capacity >= 1000:  # Valid capacity
                    rest_of_line = cap_match.group(2)
                    
                    # Extract all cost ranges from the line
                    cost_ranges = re.findall(r'\d+\.?\d*\s*[–—-]\s*\d+\.?\d*', rest_of_line)
                    
                    if len(cost_ranges) >= len(speeds):
                        # This is a data row
                        capacities.append(capacity)
                        
                        # First N values are base costs for each speed
                        row_costs = []
                        for val_str in cost_ranges[:len(speeds)]:
                            cost = clean_number(val_str)
                            row_costs.append(cost)
                        
                        cost_matrix.append(row_costs)
                        
                        # Remaining values are manual and power door costs
                        if len(cost_ranges) > len(speeds):
                            manual_idx = len(speeds)
                            power_idx = len(speeds) + 1
                            
                            if manual_idx < len(cost_ranges):
                                manual_cost = clean_number(cost_ranges[manual_idx])
                                if manual_cost:
                                    cost_per_stop_manual[capacity] = manual_cost
                            
                            if power_idx < len(cost_ranges):
                                power_cost = clean_number(cost_ranges[power_idx])
                                if power_cost:
                                    cost_per_stop_power[capacity] = power_cost
            except ValueError:
                continue
    
    if not capacities or not speeds:
        return None
    
    return {
        "type": "freight",
        "name": table_name,
        "capacities": capacities,
        "speeds": speeds,
        "costs": cost_matrix,
        "cost_per_stop_manual": cost_per_stop_manual,
        "cost_per_stop_power": cost_per_stop_power
    }

def parse_elevator_pdf(pdf_path: str, start_page: int = 519, end_page: int = 521) -> Dict[str, Any]:
    """
    Parse elevator tables from MVS PDF Section 58
    Pages 519-521 contain passenger and freight elevator tables
    """
    elevators = {
        "passenger": {},
        "freight": {}
    }
    
    print(f"Opening PDF: {pdf_path}")
    
    with pdfplumber.open(pdf_path) as pdf:
        # Section 58 typically starts around page 3 in the elevator section
        for page_num in range(start_page - 1, min(end_page, len(pdf.pages))):
            page = pdf.pages[page_num]
            text = page.extract_text()
            
            if not text:
                continue
            
            lines = text.split('\n')
            
            print(f"\n--- Page {page_num + 1} ---")
            
            # Identify table type
            current_table_name = None
            table_lines = []
            in_freight_section = False  # Track if we're in freight or passenger section
            
            for line in lines:
                line_upper = line.upper()
                
                # Skip escalator and moving walks tables for now
                if ('ESCALATOR' in line_upper or 'MOVING WALK' in line_upper) and current_table_name:
                    print(f"Skipping: {line.strip()}")
                    # End current table if we hit escalator/moving walks
                    if current_table_name and table_lines:
                        if 'FREIGHT' in current_table_name.upper():
                            result = extract_freight_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['freight'][current_table_name] = result
                        else:
                            result = extract_passenger_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['passenger'][current_table_name] = result
                    current_table_name = None
                    table_lines = []
                    continue
                
                # Identify passenger elevator main sections
                if 'PASSENGER ELEVATOR' in line_upper and '–' in line:
                    # Save previous table if exists
                    if current_table_name and table_lines:
                        if in_freight_section:
                            result = extract_freight_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['freight'][current_table_name] = result
                        else:
                            result = extract_passenger_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['passenger'][current_table_name] = result
                    
                    # Reset for new section - we'll identify the actual table type below
                    current_table_name = None
                    table_lines = []
                    in_freight_section = False
                    print(f"Found passenger section: {line.strip()}")
                
                # Identify specific elevator table types
                elif 'ELECTRIC, VARIABLE VOLTAGE CONTROL' in line_upper:
                    # Save previous table
                    if current_table_name and table_lines:
                        if in_freight_section:
                            result = extract_freight_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['freight'][current_table_name] = result
                        else:
                            result = extract_passenger_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['passenger'][current_table_name] = result
                    
                    table_lines = [line]
                    prefix = "Freight - " if in_freight_section else "Passenger - "
                    current_table_name = prefix + "Electric, Variable Voltage Control"
                    print(f"Found: {current_table_name}")
                
                elif line_upper.strip() == 'HYDRAULIC':
                    # Save previous table
                    if current_table_name and table_lines:
                        if in_freight_section:
                            result = extract_freight_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['freight'][current_table_name] = result
                        else:
                            result = extract_passenger_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['passenger'][current_table_name] = result
                    
                    table_lines = [line]
                    prefix = "Freight - " if in_freight_section else "Passenger - "
                    current_table_name = prefix + "Hydraulic"
                    print(f"Found: {current_table_name}")
                
                elif 'ELECTRIC, AC RHEOSTATIC CONTROL' in line_upper:
                    # Save previous table
                    if current_table_name and table_lines:
                        if 'FREIGHT' in current_table_name.upper():
                            result = extract_freight_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['freight'][current_table_name] = result
                        else:
                            result = extract_passenger_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['passenger'][current_table_name] = result
                    
                    table_lines = [line]
                    current_table_name = "Electric, AC Rheostatic Control"
                    print(f"Found: {current_table_name}")
                
                elif 'SELECTOMATIC' in line_upper or ('PASSENGER ELEVATOR' in line_upper and 'AUTOMATIC' in line_upper):
                    # Save previous table
                    if current_table_name and table_lines:
                        if 'FREIGHT' in current_table_name.upper():
                            result = extract_freight_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['freight'][current_table_name] = result
                        else:
                            result = extract_passenger_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['passenger'][current_table_name] = result
                    
                    table_lines = [line]
                    current_table_name = "Selectomatic/Automatic"
                    print(f"Found: {current_table_name}")
                
                # Identify freight elevator section
                elif 'FREIGHT ELEVATOR' in line_upper:
                    # Save previous table
                    if current_table_name and table_lines:
                        if in_freight_section:
                            result = extract_freight_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['freight'][current_table_name] = result
                        else:
                            result = extract_passenger_elevator_table(table_lines, current_table_name)
                            if result:
                                elevators['passenger'][current_table_name] = result
                    
                    # Reset - we'll identify type below
                    current_table_name = None
                    table_lines = []
                    in_freight_section = True
                    print(f"Found freight section: {line.strip()}")
                
                # Add line to current table
                elif current_table_name:
                    table_lines.append(line)
            
            # Save last table on page
            if current_table_name and table_lines:
                if 'FREIGHT' in current_table_name.upper():
                    result = extract_freight_elevator_table(table_lines, current_table_name)
                    if result:
                        elevators['freight'][current_table_name] = result
                else:
                    result = extract_passenger_elevator_table(table_lines, current_table_name)
                    if result:
                        elevators['passenger'][current_table_name] = result
    
    return elevators

def main():
    """Main execution function - accepts command-line arguments"""
    # Check for command-line arguments
    if len(sys.argv) < 4:
        # Default behavior - use hardcoded path
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        pdf_path = project_root / "data" / "pdfs" / "MVS.pdf"
        start_page = 519
        end_page = 521
    else:
        # Command-line usage
        pdf_path = Path(sys.argv[1])
        start_page = int(sys.argv[2])
        end_page = int(sys.argv[3])
    
    # Setup output directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    output_dir = project_root / "Tables" / "Elevators"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("MVS ELEVATOR TABLE PARSER")
    print("=" * 80)
    
    # Check if PDF exists
    if not pdf_path.exists():
        print(f"\nERROR: PDF not found at {pdf_path}")
        print("Usage: python parse-elevators.py <pdf_path> <start_page> <end_page>")
        print("Example: python parse-elevators.py MVS.pdf 519 521")
        sys.exit(1)
    
    # Parse the PDF
    print(f"\nParsing: {pdf_path}")
    print(f"Pages: {start_page}-{end_page}")
    elevators = parse_elevator_pdf(str(pdf_path), start_page, end_page)
    
    # Save to JSON
    output_file = output_dir / "elevators.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(elevators, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'=' * 80}")
    print("EXTRACTION COMPLETE")
    print(f"{'=' * 80}")
    print(f"\nOutput saved to: {output_file}")
    print(f"\nSummary:")
    print(f"  Passenger Elevator Types: {len(elevators['passenger'])}")
    for name in elevators['passenger'].keys():
        print(f"    - {name}")
    print(f"  Freight Elevator Types: {len(elevators['freight'])}")
    for name in elevators['freight'].keys():
        print(f"    - {name}")
    print(f"\nNote: Escalators and moving walks tables are skipped in this version")
    
    # Create documentation
    create_documentation(elevators, output_dir)
    
    print(f"\n{'=' * 80}\n")

def create_documentation(elevators: Dict[str, Any], output_dir: Path):
    """Create markdown documentation for the parsed elevator data"""
    doc_file = output_dir / "ELEVATORS.md"
    
    with open(doc_file, 'w', encoding='utf-8') as f:
        f.write("# Elevator and Escalator Data\n\n")
        f.write("Extracted from MVS PDF Section 58 (Elevators - Escalators)\n\n")
        
        # Passenger Elevators
        f.write("## Passenger Elevators\n\n")
        for name, data in elevators['passenger'].items():
            f.write(f"### {name}\n\n")
            f.write(f"- **Speeds**: {', '.join(map(str, data['speeds']))} feet/min\n")
            f.write(f"- **Capacities**: {', '.join(map(str, data['capacities']))} lbs\n")
            f.write(f"- **Cost Matrix**: {len(data['speeds'])} speeds × {len(data['capacities'])} capacities\n")
            f.write(f"- **Cost Per Stop**: {len(data.get('cost_per_stop', {}))} entries\n\n")
        
        # Freight Elevators
        f.write("## Freight Elevators\n\n")
        for name, data in elevators['freight'].items():
            f.write(f"### {name}\n\n")
            f.write(f"- **Capacities**: {', '.join(map(str, data['capacities']))} lbs\n")
            f.write(f"- **Speeds**: {', '.join(map(str, data['speeds']))} feet/min\n")
            f.write(f"- **Cost Matrix**: {len(data['capacities'])} capacities × {len(data['speeds'])} speeds\n")
            f.write(f"- **Cost Per Stop (Manual)**: {len(data.get('cost_per_stop_manual', {}))} entries\n")
            f.write(f"- **Cost Per Stop (Power)**: {len(data.get('cost_per_stop_power', {}))} entries\n\n")
        
        f.write("## Usage\n\n")
        f.write("### Passenger Elevator Cost Calculation\n")
        f.write("```\n")
        f.write("Total Cost = Base Cost + (Number of Stops × Cost Per Stop)\n")
        f.write("```\n\n")
        
        f.write("### Freight Elevator Cost Calculation\n")
        f.write("```\n")
        f.write("Total Cost = Base Cost + (Number of Stops × Cost Per Stop [Manual or Power])\n")
        f.write("           + Additional costs (rear doors, etc.)\n")
        f.write("```\n")
    
    print(f"Documentation saved to: {doc_file}")

if __name__ == "__main__":
    main()
