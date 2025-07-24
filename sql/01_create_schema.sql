-- Create normalized schema for property data
-- This script creates the relational structure for property records

-- Drop tables in reverse order of dependencies (if they exist)
DROP TABLE IF EXISTS property_valuations;
DROP TABLE IF EXISTS property_rehab_estimates;
DROP TABLE IF EXISTS property_hoa_data;
DROP TABLE IF EXISTS property_details;
DROP TABLE IF EXISTS properties;
DROP TABLE IF EXISTS hoa_associations;
DROP TABLE IF EXISTS valuation_types;
DROP TABLE IF EXISTS rehab_categories;

-- 1. Properties (Main entity)
CREATE TABLE properties (
    property_id INT AUTO_INCREMENT PRIMARY KEY,
    address VARCHAR(500) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    county VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    property_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_location (city, state, zip_code),
    INDEX idx_property_type (property_type)
);

-- 2. Property Details (1:1 relationship with properties)
CREATE TABLE property_details (
    detail_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    bedrooms INT,
    bathrooms DECIMAL(3,1),
    square_feet INT,
    lot_size DECIMAL(10,2),
    year_built YEAR,
    garage_spaces INT,
    basement BOOLEAN DEFAULT FALSE,
    pool BOOLEAN DEFAULT FALSE,
    fireplace BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    UNIQUE KEY unique_property_detail (property_id)
);

-- 3. HOA Associations (Master table for HOA entities)
CREATE TABLE hoa_associations (
    hoa_id INT AUTO_INCREMENT PRIMARY KEY,
    hoa_name VARCHAR(255),
    management_company VARCHAR(255),
    contact_phone VARCHAR(20),
    contact_email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Property HOA Data (Many properties can belong to one HOA)
CREATE TABLE property_hoa_data (
    hoa_data_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    hoa_id INT,
    monthly_fee DECIMAL(10,2),
    special_assessment DECIMAL(10,2),
    amenities TEXT,
    restrictions TEXT,
    effective_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    FOREIGN KEY (hoa_id) REFERENCES hoa_associations(hoa_id) ON DELETE SET NULL
);

-- 5. Valuation Types (Master table)
CREATE TABLE valuation_types (
    valuation_type_id INT AUTO_INCREMENT PRIMARY KEY,
    type_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert common valuation types
INSERT INTO valuation_types (type_name, description) VALUES
('Market Value', 'Current market value estimate'),
('Tax Assessment', 'Official tax assessment value'),
('Insurance Value', 'Replacement cost for insurance'),
('Rental Value', 'Estimated monthly rental income'),
('Quick Sale', 'Value for quick sale scenario');

-- 6. Property Valuations (Multiple valuations per property)
CREATE TABLE property_valuations (
    valuation_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    valuation_type_id INT NOT NULL,
    valuation_amount DECIMAL(12,2),
    valuation_date DATE,
    source VARCHAR(100),
    confidence_score DECIMAL(3,2), -- 0.00 to 1.00
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    FOREIGN KEY (valuation_type_id) REFERENCES valuation_types(valuation_type_id),
    INDEX idx_property_valuation (property_id, valuation_type_id),
    INDEX idx_valuation_date (valuation_date)
);

-- 7. Rehab Categories (Master table)
CREATE TABLE rehab_categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert common rehab categories
INSERT INTO rehab_categories (category_name, description) VALUES
('Kitchen', 'Kitchen renovation and upgrades'),
('Bathroom', 'Bathroom renovation and repairs'),
('Flooring', 'Flooring replacement and installation'),
('Roofing', 'Roof repairs and replacement'),
('HVAC', 'Heating, ventilation, and air conditioning'),
('Electrical', 'Electrical system upgrades and repairs'),
('Plumbing', 'Plumbing repairs and upgrades'),
('Exterior', 'Exterior painting, siding, windows'),
('Interior', 'Interior painting, drywall, fixtures'),
('Structural', 'Foundation, framing, structural repairs');

-- 8. Property Rehab Estimates (Multiple estimates per property)
CREATE TABLE property_rehab_estimates (
    estimate_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    category_id INT NOT NULL,
    estimated_cost DECIMAL(10,2),
    priority_level ENUM('LOW', 'MEDIUM', 'HIGH', 'CRITICAL') DEFAULT 'MEDIUM',
    estimated_timeline_days INT,
    contractor_notes TEXT,
    estimate_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES rehab_categories(category_id),
    INDEX idx_property_rehab (property_id, category_id),
    INDEX idx_priority (priority_level)
);

-- Create views for common queries
CREATE VIEW property_summary AS
SELECT 
    p.property_id,
    p.address,
    p.city,
    p.state,
    p.zip_code,
    pd.bedrooms,
    pd.bathrooms,
    pd.square_feet,
    pd.year_built,
    COALESCE(pv.valuation_amount, 0) as market_value,
    COALESCE(SUM(pre.estimated_cost), 0) as total_rehab_estimate
FROM properties p
LEFT JOIN property_details pd ON p.property_id = pd.property_id
LEFT JOIN property_valuations pv ON p.property_id = pv.property_id 
    AND pv.valuation_type_id = (SELECT valuation_type_id FROM valuation_types WHERE type_name = 'Market Value')
LEFT JOIN property_rehab_estimates pre ON p.property_id = pre.property_id
GROUP BY p.property_id, p.address, p.city, p.state, p.zip_code, 
         pd.bedrooms, pd.bathrooms, pd.square_feet, pd.year_built, pv.valuation_amount;