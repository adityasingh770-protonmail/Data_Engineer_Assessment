#!/usr/bin/env python3
"""
Database Setup Script
=====================
This script initializes the MySQL database schema for the property data ETL pipeline.

Usage:
    python scripts/setup_database.py
"""

import mysql.connector
from mysql.connector import Error
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_sql_file(cursor, file_path):
    """Execute SQL commands from a file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_script = file.read()
        
        # Split script into individual statements
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
        
        for statement in statements:
            if statement:
                cursor.execute(statement)
                logging.info(f"Executed: {statement[:50]}...")
        
        logging.info(f"Successfully executed {file_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error executing {file_path}: {e}")
        return False

def setup_database():
    """Initialize the database schema"""
    connection = None
    
    try:
        # Connect to database
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='home_db',
            user='db_user',
            password='6equj5_db_user',
            autocommit=True
        )
        
        cursor = connection.cursor()
        logging.info("Connected to MySQL database")
        
        # Execute schema creation script
        schema_file = 'sql/01_create_schema.sql'
        if os.path.exists(schema_file):
            if execute_sql_file(cursor, schema_file):
                logging.info("Database schema created successfully")
            else:
                logging.error("Failed to create database schema")
                return False
        else:
            logging.error(f"Schema file {schema_file} not found")
            return False
        
        # Verify tables were created
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        logging.info(f"Created {len(tables)} tables:")
        for table in tables:
            logging.info(f"  - {table[0]}")
        
        return True
        
    except Error as e:
        logging.error(f"Database error: {e}")
        return False
    
    finally:
        if connection and connection.is_connected():
            connection.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    logging.info("Starting database setup...")
    success = setup_database()
    
    if success:
        logging.info("Database setup completed successfully!")
    else:
        logging.error("Database setup failed!")
        exit(1)