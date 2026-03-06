"""
HVAC/Climate Adjustment Refinement Parser for Marshall Valuation Service
Extracts HVAC adjustment cost tables from MVS PDF

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

def parse_hvac_table(pdf_path: str, start_page: int, end_page: int) -> Dict:
    """
    Parse HVAC adjustment tables from specified page range
    
    Args:
        pdf_path: Path to MVS PDF file
        start_page: Starting page number (1-indexed for user, converted to 0-indexed)
        end_page: Ending page number (1-indexed)
    
    Returns:
        Dictionary containing parsed HVAC adjustment data and metadata
    """
    results = {
        'success': False,
        'adjustments': [],
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
            
            adjustments = []
            
            for page_num in range(start_idx, end_idx):
                page = pdf.pages[page_num]
                display_page = page_num + 1
                
                print(f"\n[*] Processing page {display_page}...")
                
                # Extract text for analysis
                page_text = page.extract_text()
                
                # TODO: Implement table parsing logic after user provides format
                page_adjustments = parse_page_hvac(page, page_text, display_page)
                
                if page_adjustments:
                    adjustments.extend(page_adjustments)
                    print(f"   [+] Found {len(page_adjustments)} HVAC adjustment entries")
                else:
                    print(f"   [!] No HVAC adjustments found on page {display_page}")
            
            # Store results
            results['success'] = True
            results['adjustments'] = adjustments
            results['metadata']['total_entries'] = len(adjustments)
            
            print(f"\n[SUCCESS] Parsing complete!")
            print(f"   Total HVAC adjustments extracted: {len(adjustments)}")
            
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


def parse_page_hvac(page, page_text: str, page_num: int) -> List[Dict]:
    """
    Parse HVAC adjustment entries from a single page
    
    TODO: Implement parsing logic based on user-provided table format
    
    Args:
        page: pdfplumber page object
        page_text: Extracted text from page
        page_num: Page number (1-indexed)
    
    Returns:
        List of HVAC adjustment dictionaries
    """
    adjustments = []
    
    # PLACEHOLDER: Waiting for table format information
    print(f"   [!] Parser not yet implemented - waiting for table format details")
    
    return adjustments


def save_to_json(adjustments: List[Dict], output_path: str) -> None:
    """
    Save parsed HVAC adjustments to JSON file
    
    Args:
        adjustments: List of adjustment dictionaries
        output_path: Path to output JSON file
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    total = len(adjustments)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'source': 'Marshall Valuation Service - Section XX',
                'description': 'HVAC/Climate adjustment costs',
                'total_entries': total,
            },
            'adjustments': adjustments
        }, f, indent=2)
    
    print(f"[SAVED] JSON: {output_file}")
    print(f"   Total entries: {total}")


def save_to_markdown(adjustments: List[Dict], output_path: str) -> None:
    """
    Save parsed HVAC adjustments to markdown file
    
    Args:
        adjustments: List of adjustment dictionaries
        output_path: Path to output markdown file
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# HVAC / CLIMATE ADJUSTMENTS\n\n")
        f.write("**Source:** Marshall Valuation Service - Section XX\n")
        f.write(f"**Total Entries:** {len(adjustments)}\n\n")
        
        if adjustments:
            f.write("## Adjustment Table\n\n")
            f.write("| Climate Zone | Building Class | Adjustment | Type | Page |\n")
            f.write("| --- | --- | --- | --- | --- |\n")
            
            for adj in adjustments:
                zone = adj.get('climate_zone', '')
                bldg_class = adj.get('building_class', '')
                value = adj.get('adjustment_value', '')
                adj_type = adj.get('adjustment_type', '')
                page = adj.get('source_page', '')
                
                f.write(f"| {zone} | {bldg_class} | {value} | {adj_type} | {page} |\n")
        else:
            f.write("*No adjustments extracted*\n")
    
    print(f"[SAVED] Markdown: {output_file}")


def main():
    """
    Main entry point for command-line usage
    """
    if len(sys.argv) < 4:
        print("Usage: python parse-hvac-refinement.py <pdf_path> <start_page> <end_page>")
        print("Example: python parse-hvac-refinement.py MVS.pdf 100 105")
        print("\nNOTE: Parser not yet fully implemented. Waiting for:")
        print("  - Section number and page range")
        print("  - Table format description")
        print("  - Pricing methodology details")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    start_page = int(sys.argv[2])
    end_page = int(sys.argv[3])
    
    # Parse the PDF
    results = parse_hvac_table(pdf_path, start_page, end_page)
    
    if results['success']:
        # Save output
        output_dir = Path(__file__).parent.parent / "Tables" / "Refinements" / "HVAC"
        
        # Save as markdown
        markdown_path = output_dir / "HVAC_ADJUSTMENTS.md"
        save_to_markdown(results['adjustments'], str(markdown_path))
        
        # Save as JSON
        json_path = output_dir / "hvac_adjustments.json"
        save_to_json(results['adjustments'], str(json_path))
        
        print("\n[SUCCESS] Processing complete!")
    else:
        print("\n[ERROR] Processing failed!")
        for error in results['errors']:
            print(f"   - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
