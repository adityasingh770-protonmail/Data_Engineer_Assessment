#!/usr/bin/env python3
"""
ETL Debug Script - Diagnose Field Mapping Issues
This script helps identify why your ETL pipeline is failing to process records
"""

import json
import pandas as pd
import os
from collections import Counter

def analyze_json_structure(json_path):
    """Analyze the structure of your JSON data"""
    print(f"🔍 Analyzing JSON structure: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    if isinstance(data, dict):
        data = [data]
    
    print(f"📊 Total records: {len(data)}")
    
    # Analyze first few records
    if len(data) > 0:
        print("\n🏠 Sample record structure (first record):")
        sample_record = data[0]
        for key, value in sample_record.items():
            value_type = type(value).__name__
            value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            print(f"  {key}: {value_type} = {value_preview}")
    
    # Get all unique field names across all records
    all_fields = set()
    for record in data:
        all_fields.update(record.keys())
    
    print(f"\n📋 All unique fields found ({len(all_fields)}):")
    for field in sorted(all_fields):
        print(f"  - {field}")
    
    # Check field frequency
    field_frequency = Counter()
    for record in data:
        field_frequency.update(record.keys())
    
    print(f"\n📈 Field frequency (top 20 most common):")
    for field, count in field_frequency.most_common(20):
        percentage = (count / len(data)) * 100
        print(f"  {field}: {count}/{len(data)} ({percentage:.1f}%)")
    
    return all_fields, data

def analyze_field_config(config_path):
    """Analyze your field configuration"""
    print(f"\n🔧 Analyzing field configuration: {config_path}")
    
    try:
        df = pd.read_excel(config_path)
        print(f"📊 Config loaded successfully: {len(df)} mapped fields")
        
        print("\n📋 Configured field mappings:")
        for _, row in df.iterrows():
            raw_field = row.get('Raw_Field_Name', '')
            target_table = row.get('Target_Table', '')
            target_column = row.get('Target_Column', '')
            print(f"  {raw_field} → {target_table}.{target_column}")
        
        return df['Raw_Field_Name'].tolist()
    
    except Exception as e:
        print(f"❌ Error loading field config: {e}")
        print("📋 Using default field mapping:")
        default_fields = ['address', 'city', 'state', 'zip', 'bedrooms', 'bathrooms', 'square_feet', 'year_built']
        for field in default_fields:
            print(f"  {field}")
        return default_fields

def compare_fields(json_fields, config_fields):
    """Compare JSON fields with configuration fields"""
    print(f"\n🔍 Field Mapping Analysis:")
    
    json_fields_set = set(json_fields)
    config_fields_set = set(config_fields)
    
    # Fields in JSON but not in config
    unmapped_fields = json_fields_set - config_fields_set
    print(f"\n❌ Fields in JSON but NOT in config ({len(unmapped_fields)}):")
    for field in sorted(unmapped_fields):
        print(f"  - {field}")
    
    # Fields in config but not in JSON
    missing_fields = config_fields_set - json_fields_set
    print(f"\n⚠️  Fields in config but NOT in JSON ({len(missing_fields)}):")
    for field in sorted(missing_fields):
        print(f"  - {field}")
    
    # Matching fields
    matching_fields = json_fields_set & config_fields_set
    print(f"\n✅ Matching fields ({len(matching_fields)}):")
    for field in sorted(matching_fields):
        print(f"  - {field}")
    
    # Analysis
    if len(matching_fields) == 0:
        print(f"\n🚨 CRITICAL ISSUE: No matching fields found!")
        print(f"   This explains why all records failed with 'No property data found'")
        print(f"   The field names in your JSON don't match your configuration.")
    
    elif len(matching_fields) < 3:
        print(f"\n⚠️  WARNING: Very few matching fields ({len(matching_fields)})")
        print(f"   This might cause most records to fail processing.")
    
    else:
        print(f"\n✅ Good: {len(matching_fields)} matching fields found")

def suggest_fixes(json_fields, config_fields):
    """Suggest fixes for field mapping issues"""
    print(f"\n💡 SUGGESTED FIXES:")
    
    json_fields_set = set(json_fields)
    config_fields_set = set(config_fields)
    matching_fields = json_fields_set & config_fields_set
    
    if len(matching_fields) == 0:
        print("\n1. UPDATE YOUR FIELD CONFIG:")
        print("   Create/update your 'Field Config.xlsx' file with these JSON field names:")
        for field in sorted(json_fields)[:10]:  # Show first 10 as example
            print(f"   Raw_Field_Name: {field}")
        
        print(f"\n2. OR CREATE A NEW MAPPING IN CODE:")
        print("   Add this to your FieldConfigLoader._create_default_mapping():")
        for field in sorted(json_fields)[:10]:
            print(f"   '{field}': {{'table': 'properties', 'column': '{field}', 'type': 'VARCHAR'}},")
    
    print(f"\n3. CHECK YOUR JSON DATA:")
    print(f"   Make sure your JSON file has the expected structure")
    print(f"   Current JSON fields vs Expected config fields mismatch")

def main():
    """Main diagnostic function"""
    print("🛠️  ETL PIPELINE DIAGNOSTIC TOOL")
    print("=" * 50)
    
    # Find JSON file
    json_files = [f for f in os.listdir('data/') if f.endswith('.json')]
    if not json_files:
        print("❌ No JSON files found in data/ directory")
        return
    
    json_path = os.path.join('data', json_files[0])
    config_path = 'data/Field Config.xlsx'
    
    # Analyze JSON structure
    json_fields, sample_data = analyze_json_structure(json_path)
    
    # Analyze field configuration
    config_fields = analyze_field_config(config_path)
    
    # Compare and suggest fixes
    compare_fields(json_fields, config_fields)
    suggest_fixes(json_fields, config_fields)
    
    print(f"\n📋 SUMMARY:")
    print(f"   JSON file: {json_path}")
    print(f"   Config file: {config_path}")
    print(f"   JSON fields: {len(json_fields)}")
    print(f"   Config fields: {len(config_fields)}")
    print(f"   Matching fields: {len(set(json_fields) & set(config_fields))}")

if __name__ == "__main__":
    main()