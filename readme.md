# Data Engineering Assessment - Property Data ETL Pipeline

## Overview

This project implements a complete ETL pipeline to normalize property data from JSON format into a relational MySQL database. The solution transforms denormalized property records containing mixed attributes (property details, HOA data, rehab estimates, valuations) into a properly normalized relational schema.

## Project Structure

```
├── data/
│   ├── [property_data].json          # Raw JSON property data
│   └── Field Config.xlsx             # Business logic mapping
├── sql/
│   ├── 01_create_schema.sql          # Database schema creation
│   └── 99_final_db_dump.sql          # Final database with optimizations
├── scripts/
│   ├── etl_pipeline.py               # Main ETL pipeline
│   ├── setup_database.py             # Database initialization
│   └── validate_data.py              # Data validation and reporting
├── docker-compose.initial.yml        # Initial database setup
├── docker-compose.final.yml          # Final database with data
├── Dockerfile.initial_db             # Initial database Docker image
├── Dockerfile.final_db               # Final database Docker image
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

## Database Schema

The normalized schema consists of the following entities:

### Core Entities
- **`properties`** - Main property records (address, location, type)
- **`property_details`** - Physical characteristics (beds, baths, sqft, etc.)

### Related Data
- **`hoa_associations`** - HOA master data
- **`property_hoa_data`** - Property-specific HOA information
- **`valuation_types`** - Types of valuations (market, tax, rental, etc.)
- **`property_valuations`** - Property value estimates
- **`rehab_categories`** - Rehabilitation work categories
- **`property_rehab_estimates`** - Renovation cost estimates

### Relationships
- Properties have a 1:1 relationship with property_details
- Properties can have multiple valuations and rehab estimates
- Multiple properties can belong to the same HOA association

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- MySQL client (optional, for direct database access)

### 1. Initialize Database

Start the MySQL database container:

```bash
docker-compose -f docker-compose.initial.yml up --build -d
```

This creates a MySQL 8.0 database with:
- Database: `home_db`
- User: `db_user`
- Password: `6equj5_db_user`
- Port: `3306`

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Create Database Schema

```bash
python scripts/setup_database.py
```

This script:
- Connects to the MySQL database
- Executes the schema creation SQL
- Verifies all tables were created successfully

### 4. Run ETL Pipeline

Place your JSON data file in the `data/` directory and run:

```bash
python scripts/etl_pipeline.py
```

The ETL pipeline will:
- Load the field configuration from the Excel file
- Read and parse the JSON property data
- Clean and validate the data
- Transform data into normalized format
- Load data into the relational database
- Handle foreign key relationships
- Log all operations and errors

### 5. Validate Results

```bash
python scripts/validate_data.py
```

This generates:
- Record counts for all tables
- Referential integrity checks
- Data quality assessments
- Summary statistics
- Validation report file

## ETL Pipeline Features

### Data Cleaning & Transformation
- **Type conversion** - Automatic data type conversion based on field configuration
- **Validation** - Range checks, format validation, null handling
- **Deduplication** - Prevents duplicate records
- **Error handling** - Comprehensive logging and rollback on failures

### Normalization Process
1. **Extract** property attributes from JSON
2. **Transform** according to business rules in field config
3. **Load** into appropriate normalized tables
4. **Link** related records via foreign keys

### Field Configuration Support
The pipeline uses `data/Field Config.xlsx` to map raw JSON fields to target database schema:
- Raw field names to target table/column mapping
- Data type specifications
- Business logic rules
- Validation constraints

### Monitoring & Logging
- Detailed logging to console and file
- Progress tracking during ETL execution
- Error reporting with rollback capability
- Data quality metrics and validation

## Database Features

### Optimizations
- **Indexes** on frequently queried columns (location, dates, foreign keys)
- **Views** for common reporting queries
- **Stored procedures** for complex operations
- **Triggers** for data validation and audit trails

### Views Available
- `property_summary` - Basic property information with valuations
- `property_financial_summary` - Financial analysis with profit margins
- `hoa_property_summary` - HOA aggregation data

### Stored Procedures
- `GetPropertyFullDetails(property_id)` - Complete property information
- `GetMarketAnalysis(city, state)` - Market statistics for location

## Data Quality Assurance

### Validation Checks
- Foreign key integrity verification
- Data range and format validation
- Completeness checks for required fields
- Duplicate detection and prevention

### Quality Metrics
- Record counts and data distribution
- Missing value analysis
- Outlier detection for numerical fields
- Referential integrity verification

## Usage Examples

### Query Property Data
```sql
-- Get all properties with market values
SELECT * FROM property_summary WHERE market_value > 100000;

-- Get properties needing high-priority rehab
SELECT p.address, rc.category_name, pre.estimated_cost
FROM properties p
JOIN property_rehab_estimates pre ON p.property_id = pre.property_id  
JOIN rehab_categories rc ON pre.category_id = rc.category_id
WHERE pre.priority_level = 'HIGH';
```

### Run Analysis
```sql
-- Market analysis for a specific city
CALL GetMarketAnalysis('Austin', 'TX');

-- Get complete property details  
CALL GetPropertyFullDetails(1);
```

## Deployment

### Final Database Setup
For the complete database with all optimizations:

```bash
docker-compose -f docker-compose.final.yml up --build -d
```

This includes:
- Complete normalized schema
- Performance optimizations (indexes, views)
- Data validation triggers
- Stored procedures for common operations

## Dependencies Justification

- **`mysql-connector-python`** - Official MySQL driver for database connectivity
- **`pandas`** - Data manipulation and Excel file reading capabilities  
- **`openpyxl`** - Excel file format support for field configuration
- **`python-dotenv`** - Environment variable management (optional)

All dependencies are lightweight and commonly used in data engineering workflows.

## Error Handling

The pipeline includes comprehensive error handling:
- Database connection failures
- File I/O errors
- Data validation failures
- Foreign key constraint violations
- Transaction rollback on errors

## Performance Considerations

- **Batch processing** for large datasets
- **Transaction management** for data consistency
- **Indexed queries** for fast lookups
- **Connection pooling** ready architecture
- **Memory-efficient** streaming for large JSON files

## Testing

Run the validation script to verify ETL results:
```bash
python scripts/validate_data.py
```

Expected outputs:
- All referential integrity checks should pass
- Data quality issues should be minimal
- Record counts should match input data
- Summary statistics should be reasonable

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Verify MySQL container is running: `docker ps`
   - Check connection parameters in scripts
   - Ensure the database and user exist

2. **ETL Pipeline Errors**
   - Check JSON file format and location
   - Verify the field configuration file exists
   - Review logs for specific error messages

3. **Data Quality Issues**
   - Run validation script to identify problems
   - Check source data for inconsistencies
   - Verify field mapping configuration

### Logs
- ETL pipeline logs: `etl_pipeline.log`
- Validation reports: `validation_report_[timestamp].txt`

## Future Enhancements

- Add support for incremental data loading
- Implement data lineage tracking
- Add more sophisticated data quality rules
- Create data visualization dashboards
- Add automated testing suite
