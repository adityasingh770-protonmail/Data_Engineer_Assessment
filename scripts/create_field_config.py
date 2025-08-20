#!/usr/bin/env python3
"""
Field Configuration Template Creator
===================================
Creates a template Excel file for field configuration mapping.

Usage:
    python scripts/create_field_config.py
"""

import pandas as pd
import os

def create_field_config_template():
    """Create a template field configuration file"""
    
    # Sample field mapping configuration
    field_mappings = [
        # Property basic information
        {
            'Raw_Field_Name': 'address',
            'Target_Table': 'properties',
            'Target_Column': 'address',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'Full street address of the property',
            'Required': True,
            'Example_Value': '123 Main St'
        },
        {
            'Raw_Field_Name': 'city',
            'Target_Table': 'properties',
            'Target_Column': 'city',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'City name where property is located',
            'Required': True,
            'Example_Value': 'Austin'
        },
        {
            'Raw_Field_Name': 'state',
            'Target_Table': 'properties',
            'Target_Column': 'state',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'State abbreviation (2 characters)',
            'Required': True,
            'Example_Value': 'TX'
        },
        {
            'Raw_Field_Name': 'zip',
            'Target_Table': 'properties',
            'Target_Column': 'zip_code',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'ZIP code (5 or 9 digits)',
            'Required': False,
            'Example_Value': '78701'
        },
        {
            'Raw_Field_Name': 'county',
            'Target_Table': 'properties',
            'Target_Column': 'county',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'County name',
            'Required': False,
            'Example_Value': 'Travis County'
        },
        {
            'Raw_Field_Name': 'latitude',
            'Target_Table': 'properties',
            'Target_Column': 'latitude',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'GPS latitude coordinate (-90 to 90)',
            'Required': False,
            'Example_Value': '30.267153'
        },
        {
            'Raw_Field_Name': 'longitude',
            'Target_Table': 'properties',
            'Target_Column': 'longitude',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'GPS longitude coordinate (-180 to 180)',
            'Required': False,
            'Example_Value': '-97.743061'
        },
        {
            'Raw_Field_Name': 'property_type',
            'Target_Table': 'properties',
            'Target_Column': 'property_type',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'Type of property (Single Family, Condo, etc.)',
            'Required': False,
            'Example_Value': 'Single Family'
        },
        
        # Property details
        {
            'Raw_Field_Name': 'bedrooms',
            'Target_Table': 'property_details',
            'Target_Column': 'bedrooms',
            'Data_Type': 'INT',
            'Business_Logic': 'Number of bedrooms (0-50)',
            'Required': False,
            'Example_Value': '3'
        },
        {
            'Raw_Field_Name': 'bathrooms',
            'Target_Table': 'property_details',
            'Target_Column': 'bathrooms',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Number of bathrooms (can be fractional)',
            'Required': False,
            'Example_Value': '2.5'
        },
        {
            'Raw_Field_Name': 'square_feet',
            'Target_Table': 'property_details',
            'Target_Column': 'square_feet',
            'Data_Type': 'INT',
            'Business_Logic': 'Living area in square feet',
            'Required': False,
            'Example_Value': '1800'
        },
        {
            'Raw_Field_Name': 'lot_size',
            'Target_Table': 'property_details',
            'Target_Column': 'lot_size',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Lot size in acres',
            'Required': False,
            'Example_Value': '0.25'
        },
        {
            'Raw_Field_Name': 'year_built',
            'Target_Table': 'property_details',
            'Target_Column': 'year_built',
            'Data_Type': 'YEAR',
            'Business_Logic': 'Year property was built (1800-2100)',
            'Required': False,
            'Example_Value': '1995'
        },
        {
            'Raw_Field_Name': 'garage_spaces',
            'Target_Table': 'property_details',
            'Target_Column': 'garage_spaces',
            'Data_Type': 'INT',
            'Business_Logic': 'Number of garage parking spaces',
            'Required': False,
            'Example_Value': '2'
        },
        {
            'Raw_Field_Name': 'basement',
            'Target_Table': 'property_details',
            'Target_Column': 'basement',
            'Data_Type': 'BOOLEAN',
            'Business_Logic': 'Whether property has a basement',
            'Required': False,
            'Example_Value': 'true'
        },
        {
            'Raw_Field_Name': 'pool',
            'Target_Table': 'property_details',
            'Target_Column': 'pool',
            'Data_Type': 'BOOLEAN',
            'Business_Logic': 'Whether property has a pool',
            'Required': False,
            'Example_Value': 'false'
        },
        {
            'Raw_Field_Name': 'fireplace',
            'Target_Table': 'property_details',
            'Target_Column': 'fireplace',
            'Data_Type': 'BOOLEAN',
            'Business_Logic': 'Whether property has a fireplace',
            'Required': False,
            'Example_Value': 'true'
        },
        
        # Valuation fields
        {
            'Raw_Field_Name': 'market_value',
            'Target_Table': 'property_valuations',
            'Target_Column': 'valuation_amount',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Current market value estimate -> Market Value type',
            'Required': False,
            'Example_Value': '350000'
        },
        {
            'Raw_Field_Name': 'tax_assessment',
            'Target_Table': 'property_valuations',
            'Target_Column': 'valuation_amount',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Tax assessment value -> Tax Assessment type',
            'Required': False,
            'Example_Value': '280000'
        },
        {
            'Raw_Field_Name': 'insurance_value',
            'Target_Table': 'property_valuations',
            'Target_Column': 'valuation_amount',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Insurance replacement value -> Insurance Value type',
            'Required': False,
            'Example_Value': '380000'
        },
        {
            'Raw_Field_Name': 'rental_estimate',
            'Target_Table': 'property_valuations',
            'Target_Column': 'valuation_amount',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Monthly rental income estimate -> Rental Value type',
            'Required': False,
            'Example_Value': '2500'
        },
        
        # HOA fields
        {
            'Raw_Field_Name': 'hoa_name',
            'Target_Table': 'hoa_associations',
            'Target_Column': 'hoa_name',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'Name of HOA association',
            'Required': False,
            'Example_Value': 'Sunset Hills HOA'
        },
        {
            'Raw_Field_Name': 'hoa_monthly_fee',
            'Target_Table': 'property_hoa_data',
            'Target_Column': 'monthly_fee',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Monthly HOA fee amount',
            'Required': False,
            'Example_Value': '150'
        },
        {
            'Raw_Field_Name': 'hoa_special_assessment',
            'Target_Table': 'property_hoa_data',
            'Target_Column': 'special_assessment',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'One-time special assessment amount',
            'Required': False,
            'Example_Value': '2000'
        },
        {
            'Raw_Field_Name': 'hoa_amenities',
            'Target_Table': 'property_hoa_data',
            'Target_Column': 'amenities',
            'Data_Type': 'TEXT',
            'Business_Logic': 'List of HOA amenities',
            'Required': False,
            'Example_Value': 'Pool, Tennis Court, Clubhouse'
        },
        {
            'Raw_Field_Name': 'hoa_management',
            'Target_Table': 'hoa_associations',
            'Target_Column': 'management_company',
            'Data_Type': 'VARCHAR',
            'Business_Logic': 'HOA management company name',
            'Required': False,
            'Example_Value': 'ABC Property Management'
        },
        
        # Rehab estimate fields
        {
            'Raw_Field_Name': 'kitchen_rehab',
            'Target_Table': 'property_rehab_estimates',
            'Target_Column': 'estimated_cost',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Kitchen renovation cost -> Kitchen category',
            'Required': False,
            'Example_Value': '15000'
        },
        {
            'Raw_Field_Name': 'bathroom_rehab',
            'Target_Table': 'property_rehab_estimates',
            'Target_Column': 'estimated_cost',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Bathroom renovation cost -> Bathroom category',
            'Required': False,
            'Example_Value': '8000'
        },
        {
            'Raw_Field_Name': 'flooring_cost',
            'Target_Table': 'property_rehab_estimates',
            'Target_Column': 'estimated_cost',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Flooring replacement cost -> Flooring category',
            'Required': False,
            'Example_Value': '5000'
        },
        {
            'Raw_Field_Name': 'roof_repair',
            'Target_Table': 'property_rehab_estimates',
            'Target_Column': 'estimated_cost',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'Roof repair cost -> Roofing category',
            'Required': False,
            'Example_Value': '12000'
        },
        {
            'Raw_Field_Name': 'hvac_cost',
            'Target_Table': 'property_rehab_estimates',
            'Target_Column': 'estimated_cost',
            'Data_Type': 'DECIMAL',
            'Business_Logic': 'HVAC system cost -> HVAC category',
            'Required': False,
            'Example_Value': '6000'
        }
    ]
    
    # Create DataFrame
    df = pd.DataFrame(field_mappings)
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Save to Excel file
    output_file = 'data/Field Config.xlsx'
    df.to_excel(output_file, index=False, sheet_name='Field Mapping')
    
    print(f"Field configuration template created: {output_file}")
    print(f"Number of field mappings: {len(field_mappings)}")
    print("\nThis template shows how to map raw JSON fields to normalized database schema.")
    print("Customize this file based on your actual JSON data structure.")
    
    return output_file

def main():
    """Main execution function"""
    print("Creating field configuration template...")
    create_field_config_template()
    print("\nField config template ready!")
    print("Edit 'data/Field Config.xlsx' to match your JSON data structure before running ETL.")

if __name__ == "__main__":
    main()