"""
Complex/Congested Sites Multiplier Parser for Marshall Valuation Service
Extracts site complexity multiplier tables from MVS PDF

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

def parse_complex_sites_multiplier_table(pdf_path: str, start_page: int, end_page: int) -> Dict:
    """Parse complex/congested sites multiplier tables from specified page range"""
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
                page_multipliers = parse_page_complex_sites(page, page_text, display_page)
                
                if page_multipliers:
                    multipliers.extend(page_multipliers)
                    print(f"   [+] Found {len(page_multipliers)} complex site multiplier entries")
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


def parse_page_complex_sites(page, page_text: str, page_num: int) -> List[Dict]:
    """Parse complex sites multiplier entries from a single page"""
    multipliers = []
    print(f"   [!] Parser not yet implemented - waiting for table format details")
    return multipliers


def save_to_json(multipliers: List[Dict], output_path: str) -> None:
    """Save parsed multipliers to JSON file"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'metadata': {
                'source': 'Marshall Valuation Service - Section XX',
                'description': 'Complex and congested site cost multipliers',
                'total_entries': len(multipliers),
            },
            'multipliers': multipliers
        }, f, indent=2)
    
    print(f"[SAVED] JSON: {output_file}")


def save_to_markdown(multipliers: List[Dict], output_path: str) -> None:
    """Save parsed multipliers to markdown file"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# COMPLEX/CONGESTED SITES MULTIPLIERS\n\n")
        f.write("**Source:** Marshall Valuation Service - Section XX\n")
        f.write(f"**Total Entries:** {len(multipliers)}\n\n")
        
        if multipliers:
            f.write("## Multiplier Table\n\n")
            f.write("| Site Condition | Description | Building Class | Multiplier | Page |\n")
            f.write("| --- | --- | --- | --- | --- |\n")
            
            for mult in multipliers:
                condition = mult.get('site_condition', '')
                desc = mult.get('description', '')
                bldg_class = mult.get('building_class', '')
                multiplier = mult.get('multiplier', '')
                page = mult.get('source_page', '')
                
                f.write(f"| {condition} | {desc} | {bldg_class} | {multiplier} | {page} |\n")
        else:
            f.write("*No multipliers extracted*\n")
    
    print(f"[SAVED] Markdown: {output_file}")


def main():
    if len(sys.argv) < 4:
        print("Usage: python parse-complex-sites-multiplier.py <pdf_path> <start_page> <end_page>")
        print("\nWaiting for table format information...")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    start_page = int(sys.argv[2])
    end_page = int(sys.argv[3])
    
    results = parse_complex_sites_multiplier_table(pdf_path, start_page, end_page)
    
    if results['success']:
        output_dir = Path(__file__).parent.parent / "Tables" / "Multipliers" / "ComplexSites"
        
        markdown_path = output_dir / "COMPLEX_SITES_MULTIPLIERS.md"
        save_to_markdown(results['multipliers'], str(markdown_path))
        
        json_path = output_dir / "complex_sites_multipliers.json"
        save_to_json(results['multipliers'], str(json_path))
        
        print("\n[SUCCESS] Processing complete!")
    else:
        print("\n[ERROR] Processing failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
