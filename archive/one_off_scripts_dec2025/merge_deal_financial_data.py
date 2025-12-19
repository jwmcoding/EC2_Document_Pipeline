#!/usr/bin/env python3
"""
Merge financial data from older deal CSV into newer deal CSV.

Strategy:
- Load older file (Aug 21) and extract financial fields into a lookup dict keyed by deal Name
- Stream through newer file (Dec 2) and enrich each row with financial data from lookup
- Write merged CSV preserving all fields from both files

Financial fields to merge:
  - Total_Proposed_Amount__c
  - Total_Final_Amount__c
  - Total_Savings_1yr__c
  - Total_Savings_3yr__c
  - NPI_Savings_Target__c
  - NPI_Savings_Target_Full_Contract_Term__c
  - Initial_Quote_Full_Contract_Term__c
  - Final_Amount_Full_Contract_Term__c
"""
import csv
import sys
from pathlib import Path
from typing import Dict, List

OLDER_FILE = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/Deal__c.csv"
NEWER_FILE = "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/deal__cs.csv"
OUTPUT_FILE = "/Users/jeffmuscarella/2025_Python/Dropbox/deal_merged_financial_data.csv"

FINANCIAL_FIELDS = [
    'Total_Proposed_Amount__c',
    'Total_Final_Amount__c',
    'Total_Savings_1yr__c',
    'Total_Savings_3yr__c',
    'NPI_Savings_Target__c',
    'NPI_Savings_Target_Full_Contract_Term__c',
    'Initial_Quote_Full_Contract_Term__c',
    'Final_Amount_Full_Contract_Term__c',
]


def load_financial_data() -> Dict[str, Dict[str, str]]:
    """Load financial fields from older file, indexed by deal Name."""
    print(f"📂 Loading financial data from {Path(OLDER_FILE).name}...")
    
    financial_lookup = {}
    
    try:
        with open(OLDER_FILE, 'r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("Could not read header from older file")
            
            # Verify financial fields exist
            missing = [field for field in FINANCIAL_FIELDS if field not in reader.fieldnames]
            if missing:
                print(f"  ⚠️  Warning: Missing fields in older file: {missing}")
            
            for i, row in enumerate(reader):
                deal_name = row.get('Name', '')
                if deal_name:
                    # Extract only financial fields
                    financial_data = {}
                    for field in FINANCIAL_FIELDS:
                        value = row.get(field, '')
                        if value is not None:
                            financial_data[field] = value
                    
                    financial_lookup[deal_name] = financial_data
                
                if (i + 1) % 10000 == 0:
                    print(f"  Loaded {i + 1} deals...", end='\r')
        
        print(f"  ✓ Loaded {len(financial_lookup)} deals with financial data")
        return financial_lookup
        
    except Exception as e:
        print(f"  ✗ Error loading financial data: {e}")
        sys.exit(1)


def merge_files(financial_lookup: Dict[str, Dict[str, str]]):
    """Stream through newer file and merge financial data."""
    print(f"\n📄 Merging financial data into {Path(NEWER_FILE).name}...")
    
    try:
        with open(NEWER_FILE, 'r', encoding='utf-8-sig', errors='ignore') as infile, \
             open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
            
            reader = csv.DictReader(infile)
            
            # Build new fieldnames: original fields + financial fields
            new_fieldnames = []
            if reader.fieldnames:
                new_fieldnames = list(reader.fieldnames)
                
                # Add financial fields that aren't already there
                for field in FINANCIAL_FIELDS:
                    if field not in new_fieldnames:
                        new_fieldnames.append(field)
            
            writer = csv.DictWriter(outfile, fieldnames=new_fieldnames)
            writer.writeheader()
            
            enriched_count = 0
            unenriched_count = 0
            
            for i, row in enumerate(reader):
                deal_name = row.get('Name', '')
                
                # Look up and merge financial data
                if deal_name in financial_lookup:
                    financial_data = financial_lookup[deal_name]
                    row.update(financial_data)
                    enriched_count += 1
                else:
                    unenriched_count += 1
                    # Set financial fields to empty string if not found
                    for field in FINANCIAL_FIELDS:
                        if field not in row:
                            row[field] = ''
                
                writer.writerow(row)
                
                if (i + 1) % 100000 == 0:
                    print(f"  Processed {i + 1} deals ({enriched_count} enriched)...", end='\r')
        
        print(f"\n  ✓ Merge complete!")
        print(f"    - Total deals in newer file: {i + 1}")
        print(f"    - Enriched with financial data: {enriched_count}")
        print(f"    - Without financial data: {unenriched_count}")
        print(f"    - Output file: {OUTPUT_FILE}")
        
        return i + 1, enriched_count, unenriched_count
        
    except Exception as e:
        print(f"  ✗ Error during merge: {e}")
        sys.exit(1)


def verify_output():
    """Verify output file and show sample."""
    print(f"\n🔍 Verifying output file...")
    
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Check header
            if not reader.fieldnames:
                print("  ✗ Output file has no header!")
                return
            
            print(f"  ✓ Output file has {len(reader.fieldnames)} columns")
            print(f"    Financial fields present in output:")
            for field in FINANCIAL_FIELDS:
                status = "✓" if field in reader.fieldnames else "✗"
                print(f"      {status} {field}")
            
            # Show sample with financial data
            print(f"\n  Sample rows with financial data:")
            sample_count = 0
            for row in reader:
                if sample_count >= 3:
                    break
                
                deal_name = row.get('Name', 'N/A')
                total_prop = row.get('Total_Proposed_Amount__c', '')
                total_final = row.get('Total_Final_Amount__c', '')
                savings_1yr = row.get('Total_Savings_1yr__c', '')
                
                if total_prop or total_final or savings_1yr:
                    print(f"    - {deal_name}")
                    if total_prop:
                        print(f"        Total Proposed: {total_prop}")
                    if total_final:
                        print(f"        Total Final: {total_final}")
                    if savings_1yr:
                        print(f"        Savings 1yr: {savings_1yr}")
                    sample_count += 1
            
            if sample_count == 0:
                print(f"    (No deals with financial data in sample)")
    
    except Exception as e:
        print(f"  ✗ Error verifying output: {e}")


def main():
    print("=" * 70)
    print("DEAL CSV MERGE: Financial Data Integration")
    print("=" * 70)
    
    # Step 1: Load financial data from older file
    financial_lookup = load_financial_data()
    
    # Step 2: Merge into newer file
    total_deals, enriched, unenriched = merge_files(financial_lookup)
    
    # Step 3: Verify output
    verify_output()
    
    print("\n" + "=" * 70)
    print("✅ MERGE COMPLETE")
    print("=" * 70)
    print(f"\nNext steps:")
    print(f"1. Review the output file: {OUTPUT_FILE}")
    print(f"2. Use this merged file as your deal metadata source")
    print(f"3. Update RawSalesforceExportConnector to use this merged file")


if __name__ == '__main__':
    main()
