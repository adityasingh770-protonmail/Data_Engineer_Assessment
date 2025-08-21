-- Final Database Dump for Property Data ETL Assessment
-- This file contains the complete schema and can be used with the final Docker setup

USE home_db;

-- Ensure we have the normalized schema
SOURCE /docker-entrypoint-initdb.d/01_create_schema.sql;

-- Create indexes for better performance
CREATE INDEX idx_properties_location ON properties(city, state, zip_code);
CREATE INDEX idx_properties_type ON properties(property_type);
CREATE INDEX idx_valuations_property_type ON property_valuations(property_id, valuation_type_id);
CREATE INDEX idx_valuations_date ON property_valuations(valuation_date);
CREATE INDEX idx_rehab_property_category ON property_rehab_estimates(property_id, category_id);
CREATE INDEX idx_rehab_priority ON property_rehab_estimates(priority_level);

-- Create additional views for reporting
CREATE OR REPLACE VIEW property_financial_summary AS
SELECT 
    p.property_id,
    p.address,
    p.city,
    p.state,
    pd.bedrooms,
    pd.bathrooms,
    pd.square_feet,
    mv.valuation_amount as market_value,
    rv.valuation_amount as rental_value,
    COALESCE(rehab.total_rehab_cost, 0) as total_rehab_cost,
    CASE 
        WHEN mv.valuation_amount > 0 AND rehab.total_rehab_cost > 0 
        THEN (mv.valuation_amount - rehab.total_rehab_cost) / mv.valuation_amount * 100
        ELSE NULL 
    END as profit_margin_percent
FROM properties p
LEFT JOIN property_details pd ON p.property_id = pd.property_id
LEFT JOIN (
    SELECT pv.property_id, pv.valuation_amount
    FROM property_valuations pv
    JOIN valuation_types vt ON pv.valuation_type_id = vt.valuation_type_id
    WHERE vt.type_name = 'Market Value'
) mv ON p.property_id = mv.property_id
LEFT JOIN (
    SELECT pv.property_id, pv.valuation_amount
    FROM property_valuations pv
    JOIN valuation_types vt ON pv.valuation_type_id = vt.valuation_type_id
    WHERE vt.type_name = 'Rental Value'
) rv ON p.property_id = rv.property_id
LEFT JOIN (
    SELECT property_id, SUM(estimated_cost) as total_rehab_cost
    FROM property_rehab_estimates
    GROUP BY property_id
) rehab ON p.property_id = rehab.property_id;

-- Create view for HOA summary
CREATE OR REPLACE VIEW hoa_property_summary AS
SELECT 
    ha.hoa_name,
    ha.management_company,
    COUNT(phd.property_id) as total_properties,
    AVG(phd.monthly_fee) as avg_monthly_fee,
    SUM(phd.special_assessment) as total_special_assessments
FROM hoa_associations ha
LEFT JOIN property_hoa_data phd ON ha.hoa_id = phd.hoa_id
GROUP BY ha.hoa_id, ha.hoa_name, ha.management_company;

-- Create stored procedures for common operations
DELIMITER //

CREATE PROCEDURE GetPropertyFullDetails(IN prop_id INT)
BEGIN
    -- Get basic property information
    SELECT 
        p.*,
        pd.bedrooms,
        pd.bathrooms,
        pd.square_feet,
        pd.lot_size,
        pd.year_built,
        pd.garage_spaces,
        pd.basement,
        pd.pool,
        pd.fireplace
    FROM properties p
    LEFT JOIN property_details pd ON p.property_id = pd.property_id
    WHERE p.property_id = prop_id;
    
    -- Get valuations
    SELECT 
        vt.type_name as valuation_type,
        pv.valuation_amount,
        pv.valuation_date,
        pv.source,
        pv.confidence_score
    FROM property_valuations pv
    JOIN valuation_types vt ON pv.valuation_type_id = vt.valuation_type_id
    WHERE pv.property_id = prop_id
    ORDER BY pv.valuation_date DESC;
    
    -- Get rehab estimates
    SELECT 
        rc.category_name,
        pre.estimated_cost,
        pre.priority_level,
        pre.estimated_timeline_days,
        pre.contractor_notes,
        pre.estimate_date
    FROM property_rehab_estimates pre
    JOIN rehab_categories rc ON pre.category_id = rc.category_id
    WHERE pre.property_id = prop_id
    ORDER BY pre.priority_level, pre.estimated_cost DESC;
    
    -- Get HOA information
    SELECT 
        ha.hoa_name,
        ha.management_company,
        ha.contact_phone,
        ha.contact_email,
        phd.monthly_fee,
        phd.special_assessment,
        phd.amenities,
        phd.restrictions
    FROM property_hoa_data phd
    JOIN hoa_associations ha ON phd.hoa_id = ha.hoa_id
    WHERE phd.property_id = prop_id;
END //

CREATE PROCEDURE GetMarketAnalysis(IN city_name VARCHAR(100), IN state_name VARCHAR(50))
BEGIN
    SELECT 
        COUNT(*) as total_properties,
        AVG(pv.valuation_amount) as avg_market_value,
        MIN(pv.valuation_amount) as min_market_value,
        MAX(pv.valuation_amount) as max_market_value,
        AVG(pd.square_feet) as avg_square_feet,
        AVG(pd.bedrooms) as avg_bedrooms,
        AVG(pd.bathrooms) as avg_bathrooms
    FROM properties p
    LEFT JOIN property_valuations pv ON p.property_id = pv.property_id
    LEFT JOIN valuation_types vt ON pv.valuation_type_id = vt.valuation_type_id
    LEFT JOIN property_details pd ON p.property_id = pd.property_id
    WHERE p.city = city_name 
    AND p.state = state_name
    AND vt.type_name = 'Market Value';
END //

DELIMITER ;

-- Create triggers for data integrity
DELIMITER //

CREATE TRIGGER update_property_timestamp
    BEFORE UPDATE ON properties
    FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END //

CREATE TRIGGER validate_property_details
    BEFORE INSERT ON property_details
    FOR EACH ROW
BEGIN
    IF NEW.bedrooms < 0 OR NEW.bedrooms > 50 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid bedroom count';
    END IF;
    
    IF NEW.bathrooms < 0 OR NEW.bathrooms > 50 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid bathroom count';
    END IF;
    
    IF NEW.square_feet IS NOT NULL AND (NEW.square_feet < 100 OR NEW.square_feet > 50000) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid square footage';
    END IF;
END //

CREATE TRIGGER validate_valuations
    BEFORE INSERT ON property_valuations
    FOR EACH ROW
BEGIN
    IF NEW.valuation_amount <= 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Valuation amount must be positive';
    END IF;
    
    IF NEW.confidence_score IS NOT NULL AND (NEW.confidence_score < 0 OR NEW.confidence_score > 1) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Confidence score must be between 0 and 1';
    END IF;
END //

DELIMITER ;

-- Grant appropriate permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON home_db.* TO 'db_user'@'%';
GRANT EXECUTE ON home_db.* TO 'db_user'@'%';

-- Final optimization
ANALYZE TABLE properties, property_details, property_valuations, property_rehab_estimates;