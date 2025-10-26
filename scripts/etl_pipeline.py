#!/usr/bin/env python3
"""
Property Data ETL Pipeline
==========================
This script reads raw JSON property data, normalizes it according to the field config,
and loads it into a MySQL database with proper relational structure.

Usage:
    python scripts/etl_pipeline.py

Requirements:
    - MySQL database running on localhost:3306
    - Raw JSON data in data/ directory
    - Field config Excel file in data/ directory
"""

import json
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class DatabaseConnection:
    """Handle MySQL database connections and operations"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host='localhost',
                port=3306,
                database='home_db',
                user='db_user',
                password='6equj5_db_user',
                autocommit=False
            )
            
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                logging.info("Successfully connected to MySQL database")
                return True
                
        except Error as e:
            logging.error(f"Error connecting to MySQL: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection closed")
    
    def execute_query(self, query: str, params: tuple = None):
        """Execute a single query"""
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Error as e:
            logging.error(f"Error executing query: {e}")
            self.connection.rollback()
            return None
    
    def execute_many(self, query: str, data: List[tuple]):
        """Execute query with multiple data sets"""
        try:
            self.cursor.executemany(query, data)
            self.connection.commit()
            return True
        except Error as e:
            logging.error(f"Error executing batch query: {e}")
            self.connection.rollback()
            return False
    
    def commit(self):
        """Commit transaction"""
        if self.connection:
            self.connection.commit()

class FieldConfigLoader:
    """Load and parse field configuration from Excel file"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.field_mapping = {}
        self.load_config()
    
    def load_config(self):
        """Load field configuration from Excel file"""
        try:
            df = pd.read_excel(self.config_path)
            logging.info(f"Loaded field config with {len(df)} fields")
            
            # Create mapping from raw field names to normalized structure
            for _, row in df.iterrows():
                raw_field = row.get('Raw_Field_Name', '')
                target_table = row.get('Target_Table', '')
                target_column = row.get('Target_Column', '')
                data_type = row.get('Data_Type', 'VARCHAR')
                business_logic = row.get('Business_Logic', '')
                
                if raw_field and target_table and target_column:
                    self.field_mapping[raw_field] = {
                        'table': target_table,
                        'column': target_column,
                        'type': data_type,
                        'logic': business_logic
                    }
                    
        except Exception as e:
            logging.error(f"Error loading field config: {e}")
            # Create default mapping if config file not available
            self._create_default_mapping()
    
    def _create_default_mapping(self):
        """Create default field mapping when config file is not available"""
        logging.warning("Using default field mapping")
        # This would need to be customized based on actual JSON structure
        self.field_mapping = {
            'address': {'table': 'properties', 'column': 'address', 'type': 'VARCHAR'},
            'city': {'table': 'properties', 'column': 'city', 'type': 'VARCHAR'},
            'state': {'table': 'properties', 'column': 'state', 'type': 'VARCHAR'},
            'zip': {'table': 'properties', 'column': 'zip_code', 'type': 'VARCHAR'},
            'bedrooms': {'table': 'property_details', 'column': 'bedrooms', 'type': 'INT'},
            'bathrooms': {'table': 'property_details', 'column': 'bathrooms', 'type': 'DECIMAL'},
            'square_feet': {'table': 'property_details', 'column': 'square_feet', 'type': 'INT'},
            'year_built': {'table': 'property_details', 'column': 'year_built', 'type': 'YEAR'},
        }

class PropertyDataTransformer:
    """Transform raw JSON data into normalized format"""
    
    def __init__(self, field_config: FieldConfigLoader):
        self.field_config = field_config
            
    def clean_and_validate_data(self, raw_data: Dict) -> Dict:
        """Clean and validate individual property record"""
        cleaned = {}

        for field, value in raw_data.items():
            if field in self.field_config.field_mapping:
                config = self.field_config.field_mapping[field]
                cleaned_value = self._clean_value(value, config['type'])
                if cleaned_value is not None:
                    table = config['table']
                    column = config['column']

                    if table not in cleaned:
                        cleaned[table] = {}
                    cleaned[table][column] = cleaned_value

        return cleaned
    
    def _clean_value(self, value: Any, data_type: str) -> Any:
        """Clean individual field value based on data type"""
        if value is None or value == '':
            return None
            
        try:
            if data_type == 'INT':
                return int(float(str(value)))
            elif data_type == 'DECIMAL':
                return float(str(value))
            elif data_type == 'YEAR':
                year = int(float(str(value)))
                return year if 1800 <= year <= 2100 else None
            elif data_type == 'BOOLEAN':
                # MySQL BOOLEAN expects 0 or 1
                if isinstance(value, bool):
                    return 1 if value else 0
                return 1 if str(value).lower() in ['true', '1', 'yes'] else 0
            elif data_type == 'VARCHAR_TO_BOOLEAN':
                # Convert Yes/No/None strings to 1/0 for MySQL BOOLEAN
                if value is None or str(value).strip() in ['None', '', 'null']:
                    return None
                str_val = str(value).lower().strip()
                return 1 if str_val in ['yes', 'true', '1'] else 0
            elif data_type == 'VARCHAR':
                return str(value).strip()[:255]  # Truncate to fit VARCHAR(255)
            else:
                return str(value)
                
        except (ValueError, TypeError):
            logging.warning(f"Could not convert {value} to {data_type}")
            return None
    
    def extract_valuations(self, raw_data: Dict) -> List[Dict]:
        """Extract valuation data from raw record - handles both flat and nested formats"""
        valuations = []

        # Handle nested array format (fake_property_data.json: "Valuation": [{...}, {...}])
        if 'Valuation' in raw_data and isinstance(raw_data['Valuation'], list):
            for val_record in raw_data['Valuation']:
                # Map each field in the valuation object to a valuation type
                valuation_type_mapping = {
                    'List_Price': 'List Price',
                    'Zestimate': 'Zestimate',
                    'ARV': 'ARV',
                    'Expected_Rent': 'Expected Rent',
                    'Rent_Zestimate': 'Rent Zestimate',
                    'Low_FMR': 'Low FMR',
                    'High_FMR': 'High FMR',
                    'Redfin_Value': 'Redfin Value',
                    'Previous_Rent': 'Previous Rent'
                }

                for field_name, valuation_type in valuation_type_mapping.items():
                    if field_name in val_record and val_record[field_name]:
                        try:
                            amount = float(val_record[field_name])
                            if amount > 0:
                                valuations.append({
                                    'type': valuation_type,
                                    'amount': amount,
                                    'date': datetime.now().date(),
                                    'source': 'ETL Import - Nested'
                                })
                        except (ValueError, TypeError):
                            continue

        # Handle flat format (sample_properties.json: "market_value": 123, "tax_assessment": 456)
        valuation_fields = {
            'market_value': 'Market Value',
            'tax_assessment': 'Tax Assessment',
            'insurance_value': 'Insurance Value',
            'rental_estimate': 'Rental Value'
        }

        for field, valuation_type in valuation_fields.items():
            if field in raw_data and raw_data[field]:
                try:
                    amount = float(raw_data[field])
                    if amount > 0:
                        valuations.append({
                            'type': valuation_type,
                            'amount': amount,
                            'date': datetime.now().date(),
                            'source': 'ETL Import - Flat'
                        })
                except (ValueError, TypeError):
                    continue

        return valuations
    
    def extract_rehab_estimates(self, raw_data: Dict) -> List[Dict]:
        """Extract rehab estimate data from raw record - handles both flat costs and nested flag format"""
        estimates = []

        # Handle nested array format (fake_property_data.json: "Rehab": [{...}, {...}])
        if 'Rehab' in raw_data and isinstance(raw_data['Rehab'], list):
            for rehab_record in raw_data['Rehab']:
                # Extract Underwriting_Rehab and Rehab_Calculation as total costs
                if 'Underwriting_Rehab' in rehab_record and rehab_record['Underwriting_Rehab']:
                    try:
                        cost = float(rehab_record['Underwriting_Rehab'])
                        if cost > 0:
                            estimates.append({
                                'category': 'Structural',
                                'cost': cost,
                                'priority': 'HIGH',
                                'date': datetime.now().date()
                            })
                    except (ValueError, TypeError):
                        pass

                # Extract flag-based rehab items
                flag_mapping = {
                    'Paint': 'Interior',
                    'Flooring_Flag': 'Flooring',
                    'Foundation_Flag': 'Structural',
                    'Roof_Flag': 'Roofing',
                    'HVAC_Flag': 'HVAC',
                    'Kitchen_Flag': 'Kitchen',
                    'Bathroom_Flag': 'Bathroom',
                    'Appliances_Flag': 'Kitchen',
                    'Windows_Flag': 'Exterior',
                    'Landscaping_Flag': 'Exterior'
                }

                for flag_field, category in flag_mapping.items():
                    if flag_field in rehab_record:
                        flag_value = rehab_record[flag_field]
                        # Only add if flag is 'Yes' or True
                        if flag_value in ['Yes', 'yes', True, 1, '1']:
                            # Estimate cost based on category (placeholder values)
                            estimated_costs = {
                                'Interior': 3000,
                                'Flooring': 5000,
                                'Structural': 15000,
                                'Roofing': 12000,
                                'HVAC': 6000,
                                'Kitchen': 15000,
                                'Bathroom': 8000,
                                'Exterior': 7000
                            }
                            estimates.append({
                                'category': category,
                                'cost': estimated_costs.get(category, 5000),
                                'priority': 'MEDIUM',
                                'date': datetime.now().date()
                            })

        # Handle flat format (sample_properties.json: "kitchen_rehab": 15000, "bathroom_rehab": 8000)
        rehab_fields = {
            'kitchen_rehab': 'Kitchen',
            'bathroom_rehab': 'Bathroom',
            'flooring_cost': 'Flooring',
            'roof_repair': 'Roofing',
            'hvac_cost': 'HVAC',
            'electrical_work': 'Electrical',
            'plumbing_work': 'Plumbing',
            'interior_paint': 'Interior',
            'exterior_paint': 'Exterior'
        }

        for field, category in rehab_fields.items():
            if field in raw_data and raw_data[field]:
                try:
                    cost = float(raw_data[field])
                    if cost > 0:
                        estimates.append({
                            'category': category,
                            'cost': cost,
                            'priority': 'MEDIUM',
                            'date': datetime.now().date()
                        })
                except (ValueError, TypeError):
                    continue

        return estimates

    def extract_hoa_data(self, raw_data: Dict) -> List[Dict]:
        """Extract HOA data from raw record - handles both flat and nested formats"""
        hoa_data_list = []

        # Handle nested array format (fake_property_data.json: "HOA": [{...}, {...}])
        if 'HOA' in raw_data and isinstance(raw_data['HOA'], list):
            for hoa_record in raw_data['HOA']:
                monthly_fee = None
                hoa_flag = hoa_record.get('HOA_Flag', 'No')

                # Extract HOA fee
                if 'HOA' in hoa_record and hoa_record['HOA']:
                    try:
                        monthly_fee = float(hoa_record['HOA'])
                    except (ValueError, TypeError):
                        continue

                # Only add if there's a fee or if HOA_Flag is Yes
                if monthly_fee and monthly_fee > 0:
                    hoa_data_list.append({
                        'monthly_fee': monthly_fee,
                        'hoa_flag': hoa_flag,
                        'date': datetime.now().date()
                    })

        # Handle flat format (sample_properties.json: "hoa_monthly_fee": 150)
        if 'hoa_monthly_fee' in raw_data and raw_data['hoa_monthly_fee']:
            try:
                fee = float(raw_data['hoa_monthly_fee'])
                if fee > 0:
                    hoa_data_list.append({
                        'monthly_fee': fee,
                        'hoa_name': raw_data.get('hoa_name'),
                        'special_assessment': raw_data.get('hoa_special_assessment'),
                        'amenities': raw_data.get('hoa_amenities'),
                        'management_company': raw_data.get('hoa_management'),
                        'date': datetime.now().date()
                    })
            except (ValueError, TypeError):
                pass

        return hoa_data_list

class PropertyETL:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.field_config = None
        self.transformer = None
        
    def setup(self, config_path: str = 'data/Field Config.xlsx'):
        """Initialize ETL components"""
        if not self.db.connect():
            raise Exception("Failed to connect to database")
            
        self.field_config = FieldConfigLoader(config_path)
        self.transformer = PropertyDataTransformer(self.field_config)
        
        logging.info("ETL pipeline setup complete")
    
    def load_json_data(self, json_path: str) -> List[Dict]:
        """Load and parse JSON data file"""
        try:
            with open(json_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            if isinstance(data, dict):
                data = [data]  # Convert single record to list
                
            logging.info(f"Loaded {len(data)} records from {json_path}")
            return data
            
        except Exception as e:
            logging.error(f"Error loading JSON data: {e}")
            return []
    
    def process_property(self, raw_property: Dict) -> Optional[int]:
        """Process single property record and return property_id"""
        try:
            # Clean and transform data
            cleaned_data = self.transformer.clean_and_validate_data(raw_property)

            if 'properties' not in cleaned_data:
                logging.warning("No property data found in record")
                return None
            
            # Insert property record
            property_id = self._insert_property(cleaned_data['properties'])
            if not property_id:
                return None
            
            # Insert related data
            if 'property_details' in cleaned_data:
                self._insert_property_details(property_id, cleaned_data['property_details'])
            
            # Insert valuations
            valuations = self.transformer.extract_valuations(raw_property)
            for valuation in valuations:
                self._insert_valuation(property_id, valuation)
            
            # Insert rehab estimates
            estimates = self.transformer.extract_rehab_estimates(raw_property)
            for estimate in estimates:
                self._insert_rehab_estimate(property_id, estimate)

            # Insert HOA data
            hoa_data_list = self.transformer.extract_hoa_data(raw_property)
            for hoa_data in hoa_data_list:
                self._insert_hoa_data(property_id, hoa_data)

            self.db.commit()
            return property_id
            
        except Exception as e:
            logging.error(f"Error processing property: {e}")
            return None
    
    def _insert_property(self, property_data: Dict) -> Optional[int]:
        """Insert property record and return property_id"""
        columns = list(property_data.keys())
        values = list(property_data.values())
        placeholders = ', '.join(['%s'] * len(values))
        column_names = ', '.join(columns)
        
        query = f"INSERT INTO properties ({column_names}) VALUES ({placeholders})"
        
        try:
            self.db.cursor.execute(query, values)
            return self.db.cursor.lastrowid
        except Error as e:
            logging.error(f"Error inserting property: {e}")
            return None
    
    def _insert_property_details(self, property_id: int, details_data: Dict):
        """Insert property details"""
        details_data['property_id'] = property_id
        
        columns = list(details_data.keys())
        values = list(details_data.values())
        placeholders = ', '.join(['%s'] * len(values))
        column_names = ', '.join(columns)
        
        query = f"INSERT INTO property_details ({column_names}) VALUES ({placeholders})"
        
        try:
            self.db.cursor.execute(query, values)
        except Error as e:
            logging.error(f"Error inserting property details: {e}")
    
    def _insert_valuation(self, property_id: int, valuation: Dict):
        """Insert property valuation"""
        # Get valuation type ID
        type_query = "SELECT valuation_type_id FROM valuation_types WHERE type_name = %s"
        result = self.db.execute_query(type_query, (valuation['type'],))
        
        if not result:
            logging.warning(f"Valuation type '{valuation['type']}' not found")
            return
        
        valuation_type_id = result[0]['valuation_type_id']
        
        query = """
        INSERT INTO property_valuations 
        (property_id, valuation_type_id, valuation_amount, valuation_date, source)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        values = (
            property_id,
            valuation_type_id,
            valuation['amount'],
            valuation['date'],
            valuation['source']
        )
        
        try:
            self.db.cursor.execute(query, values)
        except Error as e:
            logging.error(f"Error inserting valuation: {e}")
    
    def _insert_rehab_estimate(self, property_id: int, estimate: Dict):
        """Insert rehab estimate"""
        # Get category ID
        category_query = "SELECT category_id FROM rehab_categories WHERE category_name = %s"
        result = self.db.execute_query(category_query, (estimate['category'],))
        
        if not result:
            logging.warning(f"Rehab category '{estimate['category']}' not found")
            return
        
        category_id = result[0]['category_id']
        
        query = """
        INSERT INTO property_rehab_estimates 
        (property_id, category_id, estimated_cost, priority_level, estimate_date)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        values = (
            property_id,
            category_id,
            estimate['cost'],
            estimate['priority'],
            estimate['date']
        )
        
        try:
            self.db.cursor.execute(query, values)
        except Error as e:
            logging.error(f"Error inserting rehab estimate: {e}")

    def _insert_hoa_data(self, property_id: int, hoa_data: Dict):
        """Insert HOA data for property"""
        hoa_id = None

        # Check if we need to create/lookup HOA association
        if 'hoa_name' in hoa_data and hoa_data['hoa_name']:
            # Try to find existing HOA
            hoa_query = "SELECT hoa_id FROM hoa_associations WHERE hoa_name = %s"
            result = self.db.execute_query(hoa_query, (hoa_data['hoa_name'],))

            if result:
                hoa_id = result[0]['hoa_id']
            else:
                # Create new HOA association
                insert_hoa_query = """
                INSERT INTO hoa_associations (hoa_name, management_company)
                VALUES (%s, %s)
                """
                try:
                    self.db.cursor.execute(insert_hoa_query, (
                        hoa_data['hoa_name'],
                        hoa_data.get('management_company')
                    ))
                    hoa_id = self.db.cursor.lastrowid
                except Error as e:
                    logging.error(f"Error inserting HOA association: {e}")

        # Insert property HOA data
        query = """
        INSERT INTO property_hoa_data
        (property_id, hoa_id, monthly_fee, special_assessment, amenities, effective_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        values = (
            property_id,
            hoa_id,
            hoa_data.get('monthly_fee'),
            hoa_data.get('special_assessment'),
            hoa_data.get('amenities'),
            hoa_data.get('date')
        )

        try:
            self.db.cursor.execute(query, values)
        except Error as e:
            logging.error(f"Error inserting HOA data: {e}")

    def run_pipeline(self, json_path: str):
        """Execute the complete ETL pipeline"""
        logging.info("Starting ETL pipeline")

        # Load data
        raw_data = self.load_json_data(json_path)
        if not raw_data:
            logging.error("No data to process")
            return

        # Process each property
        processed_count = 0
        failed_count = 0
        total_records = len(raw_data)

        # Log progress every N records for large datasets
        progress_interval = 100 if total_records > 1000 else 10

        for i, property_record in enumerate(raw_data):
            # Only log details every N records for large datasets
            if i % progress_interval == 0 or i == total_records - 1:
                logging.info(f"Progress: {i+1}/{total_records} ({(i+1)/total_records*100:.1f}%)")

            # Disable verbose logging for large datasets
            if total_records <= 100:
                logging.info(f"Processing record {i+1}/{total_records}")
                logging.info(f"Processing record Val {i}, {property_record}")

            property_id = self.process_property(property_record)
            if property_id:
                processed_count += 1
                if total_records <= 100:
                    logging.info(f"Successfully processed property ID: {property_id}")
            else:
                failed_count += 1
                logging.warning(f"Failed to process record {i+1}")

        logging.info("=" * 80)
        logging.info(f"ETL pipeline complete for {json_path}")
        logging.info(f"Total Records: {total_records}")
        logging.info(f"Successfully Processed: {processed_count}")
        logging.info(f"Failed: {failed_count}")
        logging.info(f"Success Rate: {processed_count/total_records*100:.1f}%")
        logging.info("=" * 80)
    
    def cleanup(self):
        """Clean up resources"""
        if self.db:
            self.db.disconnect()
    
def get_data_directory() -> str:
    """
    Return absolute path to the data directory which is located one level
    up from the script (i.e. ../data relative to this script file).
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)           # one level up
    data_dir = os.path.join(parent_dir, 'data')
    return os.path.normpath(data_dir)

def main():
    """Main execution function"""
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

    etl = PropertyETL()
    try:
        etl.setup()

        dataDir = get_data_directory()
        try:
            jsonFiles = [f for f in os.listdir(dataDir) if f.lower().endswith('.json')]
        except FileNotFoundError:
            logging.error("Data directory not found: %s", dataDir)
            return

        logging.info("Found JSON files: %s", jsonFiles)
        if not jsonFiles:
            logging.error("No JSON files found in data/ directory (%s)", dataDir)
            return

        # Process ALL JSON files in the data directory
        for jsonFile in jsonFiles:
            jsonPath = os.path.join(dataDir, jsonFile)
            logging.info("=" * 80)
            logging.info("Processing file: %s", jsonPath)
            logging.info("=" * 80)

            # Run pipeline for this file
            etl.run_pipeline(jsonPath)

    except Exception as e:
        logging.error("Pipeline failed: %s", e, exc_info=True)
    finally:
        etl.cleanup()

if __name__ == "__main__":
    main()