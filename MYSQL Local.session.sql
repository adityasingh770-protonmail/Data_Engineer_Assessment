
USE Assignment;
-- drop DATABASE assignment;

CREATE TABLE properties (
    property_id VARCHAR(2000) PRIMARY KEY,
    address VARCHAR(1000),
    city VARCHAR(1000),
    state VARCHAR(1000)
);

CREATE TABLE valuations (
    valuation_id VARCHAR(1000) PRIMARY KEY,
    property_id VARCHAR(2000),
    estimated_value DECIMAL(12,2),
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);

SELECT * FROM properties;
SELECT * FROM valuations;


CREATE TABLE properties (
    id INT AUTO_INCREMENT PRIMARY KEY,
    property_id TEXT,
    address TEXT,
    city TEXT,
    state TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


