#!/usr/bin/env python3
"""
Data Validation Script
======================
This script validates the ETL results and provides data quality reports.

Usage:
    python scripts/validate_data.py
"""

import mysql.connector
from mysql.connector import Error
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataValidator:
    """Validate ETL results and generate reports"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Connect to database"""
        try:
            self.connection = mysql.connector.connect(
                host='localhost',
                port=3306,
                database='home_db',
                user='db_user',
                password='6equj5_db_user'
            )
            self.cursor = self.connection.cursor(dictionary=True)
            logging.info("Connected to database for validation")
            return True
        except Error as e:
            logging.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("Database connection closed")
    
    def get_table_counts(self):
        """Get record counts for all tables"""
        tables = [
            'properties', 'property_details', 'property_hoa_data',
            'property_valuations', 'property_rehab_estimates',
            'hoa_associations', 'valuation_types', 'rehab_categories'
        ]
        
        counts = {}
        for table in tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = self.cursor.fetchone()
                counts[table] = result['count']
            except Error as e:
                logging.error(f"Error counting {table}: {e}")
                counts[table] = 0
        
        return counts
    
    def validate_referential_integrity(self):
        """Check foreign key relationships"""
        integrity_checks = [
            {
                'name': 'Property Details Foreign Keys',
                'query': '''
                    SELECT COUNT(*) as orphaned_records
                    FROM property_details pd
                    LEFT JOIN properties p ON pd.property_id = p.property_id
                    WHERE p.property_id IS NULL
                '''
            },
            {
                'name': 'Property Valuations Foreign Keys',
                'query': '''
                    SELECT COUNT(*) as orphaned_records
                    FROM property_valuations pv
                    LEFT JOIN properties p ON pv.property_id = p.property_id
                    WHERE p.property_id IS NULL
                '''
            },
            {
                'name': 'Property Rehab Estimates Foreign Keys',
                'query': '''
                    SELECT COUNT(*) as orphaned_records
                    FROM property_rehab_estimates pre
                    LEFT JOIN properties p ON pre.property_id = p.property_id
                    WHERE p.property_id IS NULL
                '''
            }
        ]
        
        results = {}
        for check in integrity_checks:
            try:
                self.cursor.execute(check['query'])
                result = self.cursor.fetchone()
                results[check['name']] = result['orphaned_records']
            except Error as e:
                logging.error(f"Error in integrity check {check['name']}: {e}")
                results[check['name']] = -1
        
        return results
    
    def check_data_quality(self):
        """Perform data quality checks"""
        quality_checks = [
            {
                'name': 'Properties with missing address',
                'query': 'SELECT COUNT(*) as count FROM properties WHERE address IS NULL OR address = ""'
            },
            {
                'name': 'Properties with invalid coordinates',
                'query': '''
                    SELECT COUNT(*) as count FROM properties 
                    WHERE latitude < -90 OR latitude > 90 
                    OR longitude < -180 OR longitude > 180
                '''
            },
            {
                'name': 'Property details with invalid bedrooms',
                'query': 'SELECT COUNT(*) as count FROM property_details WHERE bedrooms < 0 OR bedrooms > 20'
            },
            {
                'name': 'Property details with invalid bathrooms',
                'query': 'SELECT COUNT(*) as count FROM property_details WHERE bathrooms < 0 OR bathrooms > 20'
            },
            {
                'name': 'Valuations with zero or negative amounts',
                'query': 'SELECT COUNT(*) as count FROM property_valuations WHERE valuation_amount <= 0'
            },
            {
                'name': 'Rehab estimates with zero or negative costs',
                'query': 'SELECT COUNT(*) as count FROM property_rehab_estimates WHERE estimated_cost <= 0'
            }
        ]
        
        results = {}
        for check in quality_checks:
            try:
                self.cursor.execute(check['query'])
                result = self.cursor.fetchone()
                results[check['name']] = result['count']
            except Error as e:
                logging.error(f"Error in quality check {check['name']}: {e}")
                results[check['name']] = -1
        
        return results
    
    def generate_summary_statistics(self):
        """Generate summary statistics for the dataset"""
        stats_queries = [
            {
                'name': 'Average Property Value',
                'query': '''
                    SELECT AVG(valuation_amount) as avg_value
                    FROM property_valuations pv
                    JOIN valuation_types vt ON pv.valuation_type_id = vt.valuation_type_id
                    WHERE vt.type_name = 'Market Value'
                '''
            },
            {
                'name': 'Average Rehab Cost per Property',
                'query': '''
                    SELECT AVG(total_rehab) as avg_rehab
                    FROM (
                        SELECT property_id, SUM(estimated_cost) as total_rehab
                        FROM property_rehab_estimates
                        GROUP BY property_id
                    ) as rehab_totals
                '''
            },
            {
                'name': 'Properties by State Distribution',
                'query': '''
                    SELECT state, COUNT(*) as property_count
                    FROM properties
                    WHERE state IS NOT NULL
                    GROUP BY state
                    ORDER BY property_count DESC
                    LIMIT 10
                '''
            },
            {
                'name': 'Most Common Property Types',
                'query': '''
                    SELECT property_type, COUNT(*) as count
                    FROM properties
                    WHERE property_type IS NOT NULL
                    GROUP BY property_type
                    ORDER BY count DESC
                    LIMIT 5
                '''
            }
        ]
        
        results = {}
        for stat in stats_queries:
            try:
                self.cursor.execute(stat['query'])
                if 'Distribution' in stat['name'] or 'Common' in stat['name']:
                    results[stat['name']] = self.cursor.fetchall()
                else:
                    result = self.cursor.fetchone()
                    results[stat['name']] = result
            except Error as e:
                logging.error(f"Error in statistics query {stat['name']}: {e}")
                results[stat['name']] = None
        
        return results
    
    def run_validation(self):
        """Run complete validation suite"""
        if not self.connect():
            return False
        
        logging.info("Starting data validation...")
        
        # Get table counts
        logging.info("=== TABLE RECORD COUNTS ===")
        counts = self.get_table_counts()
        for table, count in counts.items():
            logging.info(f"{table}: {count:,} records")
        
        # Check referential integrity
        logging.info("\n=== REFERENTIAL INTEGRITY CHECKS ===")
        integrity_results = self.validate_referential_integrity()
        for check, orphaned in integrity_results.items():
            if orphaned == 0:
                logging.info(f"✓ {check}: PASSED")
            else:
                logging.warning(f"✗ {check}: {orphaned} orphaned records found")
        
        # Data quality checks
        logging.info("\n=== DATA QUALITY CHECKS ===")
        quality_results = self.check_data_quality()
        for check, issues in quality_results.items():
            if issues == 0:
                logging.info(f"✓ {check}: PASSED")
            else:
                logging.warning(f"✗ {check}: {issues} issues found")
        
        # Summary statistics
        logging.info("\n=== SUMMARY STATISTICS ===")
        stats = self.generate_summary_statistics()
        
        for stat_name, result in stats.items():
            if result is None:
                logging.error(f"Failed to calculate: {stat_name}")
                continue
                
            if isinstance(result, list):
                logging.info(f"{stat_name}:")
                for item in result:
                    logging.info(f"  {item}")
            else:
                logging.info(f"{stat_name}: {result}")
        
        # Generate validation report
        self.generate_validation_report(counts, integrity_results, quality_results, stats)
        
        self.disconnect()
        return True
    
    def generate_validation_report(self, counts, integrity, quality, stats):
        """Generate a validation report file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"validation_report_{timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write("DATA VALIDATION REPORT\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("TABLE RECORD COUNTS\n")
            f.write("-" * 20 + "\n")
            for table, count in counts.items():
                f.write(f"{table}: {count:,}\n")
            
            f.write("\nREFERENTIAL INTEGRITY\n")
            f.write("-" * 20 + "\n")
            for check, orphaned in integrity.items():
                status = "PASSED" if orphaned == 0 else f"{orphaned} ISSUES"
                f.write(f"{check}: {status}\n")
            
            f.write("\nDATA QUALITY CHECKS\n")
            f.write("-" * 20 + "\n")
            for check, issues in quality.items():
                status = "PASSED" if issues == 0 else f"{issues} ISSUES"
                f.write(f"{check}: {status}\n")
            
            f.write("\nSUMMARY STATISTICS\n")
            f.write("-" * 20 + "\n")
            for stat_name, result in stats.items():
                if isinstance(result, list):
                    f.write(f"{stat_name}:\n")
                    for item in result:
                        f.write(f"  {item}\n")
                else:
                    f.write(f"{stat_name}: {result}\n")
        
        logging.info(f"Validation report saved to: {report_file}")

def main():
    """Main execution function"""
    validator = DataValidator()
    
    try:
        success = validator.run_validation()
        if success:
            logging.info("Data validation completed successfully!")
        else:
            logging.error("Data validation failed!")
            exit(1)
    except Exception as e:
        logging.error(f"Validation error: {e}")
        exit(1)

if __name__ == "__main__":
    main()