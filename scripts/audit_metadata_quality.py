#!/usr/bin/env python3
"""
Metadata Quality Audit Script

Comprehensive check of discovery JSON metadata before processing.
Identifies potential issues that could cause processing failures or
poor data quality in Pinecone.

Usage:
    python scripts/audit_metadata_quality.py [discovery_json_path]
    
    Default: production_august_discovery_12_13_2025.json
"""

import json
import sys
from pathlib import Path
from collections import Counter
from typing import Dict, List, Any, Optional
import re


def load_discovery(path: str) -> Dict[str, Any]:
    """Load discovery JSON file."""
    with open(path, 'r') as f:
        return json.load(f)


def is_problematic_string(val: Any) -> Optional[str]:
    """Check if value is a problematic string that should be empty."""
    if val is None:
        return None
    str_val = str(val).strip().lower()
    if str_val in ('nan', 'none', 'null', 'undefined', 'n/a'):
        return str_val
    return None


def has_special_chars(text: str) -> bool:
    """Check if text has special characters that might cause issues."""
    # Characters that have caused Pinecone issues
    problematic = re.compile(r'[^\x00-\x7F]')  # Non-ASCII
    return bool(problematic.search(text))


def audit_metadata(discovery: Dict[str, Any]) -> Dict[str, Any]:
    """Run comprehensive metadata audit."""
    docs = discovery.get('documents', [])
    total = len(docs)
    
    results = {
        'total_documents': total,
        'issues': {},
        'warnings': {},
        'stats': {},
    }
    
    # Issue counters
    nan_strings = Counter()
    none_strings = Counter()
    empty_required = Counter()
    special_char_files = []
    long_text_fields = Counter()
    invalid_dates = Counter()
    zero_amounts = Counter()
    id_fallback_names = {'client': 0, 'vendor': 0}
    
    # Fields to check
    required_fields = ['deal_id', 'client_name', 'vendor_name']
    text_fields = ['deal_subject', 'current_narrative', 'customer_comments']
    date_fields = ['contract_start', 'contract_end', 'deal_start_date']
    amount_fields = ['proposed_amount', 'final_amount', 'savings_1yr', 'savings_3yr']
    all_fields = [
        'deal_id', 'client_name', 'vendor_name', 'deal_status', 'deal_subject',
        'proposed_amount', 'final_amount', 'savings_achieved', 'savings_1yr',
        'contract_term', 'contract_start', 'contract_end', 'report_type',
        'project_type', 'npi_analyst', 'deal_start_date', 'current_narrative',
        'competition', 'time_pressure', 'advisor_network_used'
    ]
    
    for doc in docs:
        dm = doc.get('deal_metadata', {})
        fi = doc.get('file_info', {})
        
        # Check for special characters in filename
        filename = fi.get('name', '')
        if has_special_chars(filename):
            special_char_files.append(filename)
        
        # Check each field
        for field in all_fields:
            val = dm.get(field)
            
            # Check for problematic strings
            prob = is_problematic_string(val)
            if prob == 'nan':
                nan_strings[field] += 1
            elif prob in ('none', 'null'):
                none_strings[field] += 1
            
            # Check required fields
            if field in required_fields:
                if not val or is_problematic_string(val):
                    empty_required[field] += 1
            
            # Check text field lengths
            if field in text_fields and val:
                str_val = str(val)
                if len(str_val) > 500:
                    long_text_fields[field] += 1
            
            # Check date formats
            if field in date_fields and val:
                str_val = str(val).strip()
                if str_val and str_val.lower() not in ('nan', 'none', ''):
                    # Valid date patterns
                    date_patterns = [
                        r'^\d{1,2}/\d{1,2}/\d{2,4}',  # M/D/YY or MM/DD/YYYY
                        r'^\d{4}-\d{2}-\d{2}',        # ISO format
                    ]
                    if not any(re.match(p, str_val) for p in date_patterns):
                        invalid_dates[field] += 1
            
            # Check amounts
            if field in amount_fields:
                try:
                    if float(val or 0) == 0:
                        zero_amounts[field] += 1
                except (ValueError, TypeError):
                    pass
        
        # Check for ID fallback names
        client_name = str(dm.get('client_name', ''))
        vendor_name = str(dm.get('vendor_name', ''))
        if client_name.startswith('Client-'):
            id_fallback_names['client'] += 1
        if vendor_name.startswith('Vendor-'):
            id_fallback_names['vendor'] += 1
    
    # Compile results
    results['issues'] = {
        'nan_strings': dict(nan_strings),
        'none_strings': dict(none_strings),
        'empty_required_fields': dict(empty_required),
        'special_char_filenames': len(special_char_files),
        'special_char_samples': special_char_files[:10],
        'invalid_date_formats': dict(invalid_dates),
        'id_fallback_names': id_fallback_names,
    }
    
    results['warnings'] = {
        'long_text_fields': dict(long_text_fields),
        'zero_amounts': dict(zero_amounts),
    }
    
    # Calculate field population stats
    field_stats = {}
    for field in all_fields:
        populated = sum(1 for d in docs 
                       if d.get('deal_metadata', {}).get(field) 
                       and not is_problematic_string(d.get('deal_metadata', {}).get(field)))
        field_stats[field] = {
            'populated': populated,
            'empty': total - populated,
            'rate': round(populated / total * 100, 1) if total > 0 else 0
        }
    results['stats']['field_population'] = field_stats
    
    return results


def print_report(results: Dict[str, Any]):
    """Print formatted audit report."""
    total = results['total_documents']
    
    print('=' * 70)
    print('METADATA QUALITY AUDIT REPORT')
    print('=' * 70)
    print(f'\nTotal Documents: {total:,}')
    
    # Critical Issues
    print('\n' + '=' * 70)
    print('🔴 CRITICAL ISSUES (will cause failures)')
    print('=' * 70)
    
    issues = results['issues']
    has_critical = False
    
    # Special characters
    if issues['special_char_filenames'] > 0:
        has_critical = True
        print(f"\n⚠️  Files with special characters: {issues['special_char_filenames']}")
        print("    These may fail Pinecone upsert:")
        for f in issues['special_char_samples'][:5]:
            print(f"    - {f}")
    
    # Empty required fields
    if issues['empty_required_fields']:
        has_critical = True
        print("\n⚠️  Empty required fields:")
        for field, count in issues['empty_required_fields'].items():
            pct = count / total * 100
            print(f"    - {field}: {count:,} ({pct:.1f}%)")
    
    if not has_critical:
        print("\n✅ No critical issues found")
    
    # Data Quality Issues
    print('\n' + '=' * 70)
    print('🟡 DATA QUALITY ISSUES (will create bad data)')
    print('=' * 70)
    
    # nan/none strings
    if issues['nan_strings']:
        print("\n⚠️  Fields with 'nan' string values:")
        for field, count in sorted(issues['nan_strings'].items(), key=lambda x: -x[1]):
            if count > 0:
                pct = count / total * 100
                print(f"    - {field}: {count:,} ({pct:.1f}%)")
    
    if issues['none_strings']:
        print("\n⚠️  Fields with 'None' string values:")
        for field, count in sorted(issues['none_strings'].items(), key=lambda x: -x[1]):
            if count > 0:
                pct = count / total * 100
                print(f"    - {field}: {count:,} ({pct:.1f}%)")
    
    if issues['id_fallback_names']['client'] > 0 or issues['id_fallback_names']['vendor'] > 0:
        print("\n⚠️  Names using ID fallback (not resolved to actual names):")
        print(f"    - client_name: {issues['id_fallback_names']['client']:,}")
        print(f"    - vendor_name: {issues['id_fallback_names']['vendor']:,}")
    
    if not issues['nan_strings'] and not issues['none_strings'] and \
       issues['id_fallback_names']['client'] == 0 and issues['id_fallback_names']['vendor'] == 0:
        print("\n✅ No data quality issues found")
    
    # Warnings
    print('\n' + '=' * 70)
    print('🟢 WARNINGS (acceptable but worth noting)')
    print('=' * 70)
    
    warnings = results['warnings']
    
    if warnings['long_text_fields']:
        print("\nℹ️  Long text fields (>500 chars, will be truncated):")
        for field, count in warnings['long_text_fields'].items():
            print(f"    - {field}: {count:,}")
    
    if warnings['zero_amounts']:
        print("\nℹ️  Zero financial amounts (normal for some deals):")
        for field, count in sorted(warnings['zero_amounts'].items(), key=lambda x: -x[1]):
            pct = count / total * 100
            print(f"    - {field}: {count:,} ({pct:.1f}%)")
    
    # Field Population Summary
    print('\n' + '=' * 70)
    print('📊 FIELD POPULATION SUMMARY')
    print('=' * 70)
    print(f"\n{'Field':<25} {'Populated':>12} {'Rate':>10}")
    print('-' * 50)
    
    stats = results['stats']['field_population']
    for field in sorted(stats.keys(), key=lambda x: -stats[x]['rate']):
        s = stats[field]
        status = '✅' if s['rate'] > 80 else '⚠️' if s['rate'] > 50 else '❌'
        print(f"{field:<25} {s['populated']:>12,} {s['rate']:>9.1f}% {status}")
    
    # Final verdict
    print('\n' + '=' * 70)
    if has_critical:
        print('❌ VERDICT: Fix critical issues before processing')
    elif issues['nan_strings'] or issues['none_strings']:
        print('⚠️  VERDICT: Data quality issues exist but _sanitize_str() should handle them')
    else:
        print('✅ VERDICT: Ready to process!')
    print('=' * 70)


def main():
    # Get discovery file path
    if len(sys.argv) > 1:
        discovery_path = sys.argv[1]
    else:
        discovery_path = 'production_august_discovery_12_13_2025.json'
    
    if not Path(discovery_path).exists():
        print(f"❌ Discovery file not found: {discovery_path}")
        sys.exit(1)
    
    print(f"Loading: {discovery_path}")
    discovery = load_discovery(discovery_path)
    
    print("Running audit...")
    results = audit_metadata(discovery)
    
    print_report(results)
    
    # Save results to JSON for reference
    output_path = Path(discovery_path).stem + '_audit_results.json'
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nDetailed results saved to: {output_path}")


if __name__ == '__main__':
    main()



