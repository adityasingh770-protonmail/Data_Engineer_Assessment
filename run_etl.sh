#!/bin/bash
# Complete ETL Pipeline Execution Script

echo "=== Property Data ETL Pipeline ==="
echo "Starting complete ETL process..."

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Function to check if database is ready
wait_for_db() {
    echo -e "${YELLOW}Waiting for database to be ready...${NC}"
    
    # First, wait for the container to be healthy
    echo "Checking container status..."
    for i in {1..60}; do
        if docker ps --filter "name=mysql_ctn" --filter "status=running" | grep -q mysql_ctn; then
            echo "Container is running"
            break
        fi
        echo "Waiting for container to start... ($i/60)"
        sleep 2
    done
    
    # Now wait for MySQL service to be ready
    echo "Waiting for MySQL service to be ready..."
    for i in {1..60}; do
        # Try to connect using docker exec first
        if docker exec mysql_ctn mysql -u db_user -p6equj5_db_user -e "SELECT 1;" home_db > /dev/null 2>&1; then
            echo -e "${GREEN}Database is ready!${NC}"
            return 0
        fi
        
        # Check if MySQL is at least starting
        if docker exec mysql_ctn mysqladmin -u root -p6equj5_root ping > /dev/null 2>&1; then
            echo "MySQL is starting... checking database access ($i/60)"
        else
            echo "Waiting for MySQL to start... ($i/60)"
        fi
        sleep 3
    done
    
    echo -e "${RED}Database failed to start within timeout${NC}"
    echo "Checking container logs:"
    docker logs mysql_ctn --tail 20
    return 1
}

# Function to check if MySQL client is installed
check_mysql_client() {
    if ! command -v mysql &> /dev/null; then
        echo -e "${YELLOW}MySQL client not found. Installing via Docker exec instead.${NC}"
        return 1
    fi
    return 0
}

# Step 1: Start MySQL Database
echo -e "${YELLOW}Step 1: Starting MySQL database...${NC}"

# Stop any existing container
docker stop mysql_ctn > /dev/null 2>&1
docker rm mysql_ctn > /dev/null 2>&1

# Start fresh
docker-compose -f docker-compose.initial.yml up --build -d

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Database container started${NC}"
else
    echo -e "${RED}✗ Failed to start database${NC}"
    exit 1
fi

# Wait for database to be ready
if ! wait_for_db; then
    echo -e "${RED}Database setup failed${NC}"
    exit 1
fi

# Step 2: Install Python Dependencies
echo -e "${YELLOW}Step 2: Installing Python dependencies...${NC}"
pip install -r requirements.txt > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Dependencies installed${NC}"
else
    echo -e "${RED}✗ Failed to install dependencies${NC}"
    exit 1
fi

# Step 3: Setup Database Schema
echo -e "${YELLOW}Step 3: Creating database schema...${NC}"
python scripts/setup_database.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Database schema created${NC}"
else
    echo -e "${RED}✗ Schema creation failed${NC}"
    echo "Trying to diagnose the issue..."
    
    # Check if we can connect to database
    echo "Testing database connection..."
    docker exec mysql_ctn mysql -u db_user -p6equj5_db_user -e "SHOW DATABASES;" 2>&1
    
    exit 1
fi

# Step 4: Check for data files
echo -e "${YELLOW}Step 4: Checking for data files...${NC}"
if [ ! -d "data" ]; then
    mkdir data
    echo -e "${YELLOW}Created data/ directory${NC}"
fi

JSON_FILES=$(find data/ -name "*.json" 2>/dev/null | wc -l)
if [ $JSON_FILES -eq 0 ]; then
    echo -e "${YELLOW}Warning: No JSON files found in data/ directory${NC}"
    echo "Would you like to generate sample data for testing? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo "Generating sample data..."
        python scripts/generate_sample_data.py --count 50
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Sample data generated${NC}"
        else
            echo -e "${RED}✗ Failed to generate sample data${NC}"
        fi
    else
        echo "Please place your property data JSON files in the data/ directory"
        echo "Example: data/property_data.json"
        read -p "Press Enter when ready to continue..."
    fi
fi

# Step 5: Run ETL Pipeline
echo -e "${YELLOW}Step 5: Running ETL pipeline...${NC}"
python scripts/etl_pipeline.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ ETL pipeline completed successfully${NC}"
else
    echo -e "${RED}✗ ETL pipeline failed${NC}"
    echo "Check etl_pipeline.log for details"
    if [ -f "etl_pipeline.log" ]; then
        echo "Last few log entries:"
        tail -10 etl_pipeline.log
    fi
    exit 1
fi

# Step 6: Run Data Validation
echo -e "${YELLOW}Step 6: Running data validation...${NC}"
python scripts/validate_data.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Data validation completed${NC}"
else
    echo -e "${YELLOW}! Data validation completed with warnings${NC}"
    echo "Check validation report for details"
fi

# Step 7: Display Summary
echo -e "${YELLOW}Step 7: Generating summary...${NC}"

# Get table counts using docker exec
echo -e "${GREEN}=== ETL PIPELINE SUMMARY ===${NC}"
docker exec mysql_ctn mysql -u db_user -p6equj5_db_user home_db -e "
SELECT 'Properties' as Table_Name, COUNT(*) as Record_Count FROM properties
UNION ALL
SELECT 'Property Details', COUNT(*) FROM property_details  
UNION ALL
SELECT 'Property Valuations', COUNT(*) FROM property_valuations
UNION ALL
SELECT 'Rehab Estimates', COUNT(*) FROM property_rehab_estimates
UNION ALL  
SELECT 'HOA Data', COUNT(*) FROM property_hoa_data;
" 2>/dev/null

echo ""
echo -e "${GREEN}Pipeline completed successfully!${NC}"
echo ""
echo "Next steps:"
echo "1. Review validation report: validation_report_*.txt"
echo "2. Check ETL logs: etl_pipeline.log"  
echo "3. Connect to database:"
echo "   docker exec -it mysql_ctn mysql -u db_user -p6equj5_db_user home_db"
echo "4. Run sample queries to verify data"
echo ""
echo "To create final database with optimizations:"
echo "docker-compose -f docker-compose.final.yml up --build -d"

echo ""
echo "Database connection details:"
echo "Host: localhost"
echo "Port: 3306"
echo "Database: home_db"
echo "User: db_user"
echo "Password: 6equj5_db_user"