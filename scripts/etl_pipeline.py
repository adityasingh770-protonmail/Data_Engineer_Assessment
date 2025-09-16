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
            logging.info(f"DEBUG field: {field} {value}") 
            if field in self.field_config.field_mapping:
                logging.info(f"DEBUG field_mapping found for: {field}")
                config = self.field_config.field_mapping[field]
                logging.info(f"DEBUG config: {config}")
                cleaned_value = self._clean_value(value, config['type'])
                logging.info(f"DEBUG cleaned_value: {cleaned_value}")
                if cleaned_value is not None:
                    table = config['table']
                    column = config['column']
                
                    if table not in cleaned:
                        logging.info(f"DEBUG creating new table: {table}")
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
                return bool(value) if isinstance(value, bool) else str(value).lower() in ['true', '1', 'yes']
            elif data_type == 'VARCHAR':
                return str(value).strip()[:255]  # Truncate to fit VARCHAR(255)
            else:
                return str(value)
                
        except (ValueError, TypeError):
            logging.warning(f"Could not convert {value} to {data_type}")
            return None
    
    def extract_valuations(self, raw_data: Dict) -> List[Dict]:
        """Extract valuation data from raw record"""
        valuations = []
        
        # Look for various valuation fields
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
                            'source': 'ETL Import'
                        })
                except (ValueError, TypeError):
                    continue
        
        return valuations
    
    def extract_rehab_estimates(self, raw_data: Dict) -> List[Dict]:
        """Extract rehab estimate data from raw record"""
        estimates = []
        
        # Look for rehab-related fields
        rehab_fields = {
            'kitchen_rehab': 'Kitchen',
            'bathroom_rehab': 'Bathroom',
            'flooring_cost': 'Flooring',
            'roof_repair': 'Roofing',
            'hvac_cost': 'HVAC'
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
            logging.info(f"DEBUG {cleaned_data}")
            
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
        
        for i, property_record in enumerate(raw_data):
            logging.info(f"Processing record {i+1}/{len(raw_data)}")
            logging.info(f"Processing record Val {i}, {property_record}")
            
            property_id = self.process_property(property_record)
            if property_id:
                processed_count += 1
                logging.info(f"Successfully processed property ID: {property_id}")
            else:
                failed_count += 1
        
        logging.info(f"ETL pipeline complete. Processed: {processed_count}, Failed: {failed_count}")
    
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

        jsonPath = os.path.join(dataDir, jsonFiles[0])
        logging.info("Processing file: %s", jsonPath)

        # Run pipeline
        etl.run_pipeline(jsonPath)

    except Exception as e:
        logging.error("Pipeline failed: %s", e, exc_info=True)
    finally:
        etl.cleanup()

if __name__ == "__main__":
    main()