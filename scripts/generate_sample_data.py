#!/usr/bin/env python3
"""
Sample Data Generator
====================
Generates sample property JSON data for testing the ETL pipeline.

Usage:
    python scripts/generate_sample_data.py [--count 100] [--output data/sample_properties.json]
"""

import json
import random
import argparse
from datetime import datetime, timedelta
import os

class PropertyDataGenerator:
    """Generate realistic sample property data"""
    
    def __init__(self):
        self.cities = [
            {"city": "Austin", "state": "TX", "zip_base": "78700"},
            {"city": "Dallas", "state": "TX", "zip_base": "75200"}, 
            {"city": "Houston", "state": "TX", "zip_base": "77000"},
            {"city": "Atlanta", "state": "GA", "zip_base": "30300"},
            {"city": "Phoenix", "state": "AZ", "zip_base": "85000"},
            {"city": "Denver", "state": "CO", "zip_base": "80200"},
            {"city": "Nashville", "state": "TN", "zip_base": "37200"},
            {"city": "Charlotte", "state": "NC", "zip_base": "28200"},
        ]
        
        self.street_names = [
            "Main St", "Oak Ave", "Pine Dr", "Maple Way", "Cedar Ln",
            "Elm St", "Park Ave", "First St", "Second St", "Third St",
            "Highland Dr", "Sunset Blvd", "River Rd", "Hill St", "Valley Dr"
        ]
        
        self.property_types = [
            "Single Family", "Townhouse", "Condo", "Duplex", "Multi-Family"
        ]
        
        self.hoa_names = [
            "Sunset Hills HOA", "Oak Ridge Community", "Pine Valley Association",
            "Maple Grove HOA", "Cedar Creek Community", "Riverside HOA"
        ]

    def generate_address(self, location):
        """Generate a realistic address"""
        number = random.randint(100, 9999)
        street = random.choice(self.street_names)
        zip_code = str(int(location["zip_base"]) + random.randint(0, 99)).zfill(5)
        
        return {
            "address": f"{number} {street}",
            "city": location["city"],
            "state": location["state"],
            "zip": zip_code,
            "county": f"{location['city']} County"
        }

    def generate_property_details(self):
        """Generate property physical details"""
        bedrooms = random.choices([2, 3, 4, 5, 6], weights=[10, 40, 30, 15, 5])[0]
        bathrooms = random.choice([1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0])
        
        # Square footage based on bedrooms
        base_sqft = bedrooms * 400
        square_feet = random.randint(base_sqft, base_sqft + 800)
        
        year_built = random.randint(1970, 2023)
        
        return {
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "square_feet": square_feet,
            "lot_size": round(random.uniform(0.1, 2.0), 2),
            "year_built": year_built,
            "garage_spaces": random.choice([0, 1, 2, 3]),
            "basement": random.choice([True, False]),
            "pool": random.choice([True, False]) if random.random() > 0.7 else False,
            "fireplace": random.choice([True, False]) if random.random() > 0.6 else False,
            "property_type": random.choice(self.property_types)
        }

    def generate_coordinates(self, location):
        """Generate realistic coordinates for the location"""
        # Approximate coordinates for each city (with some variance)
        coords = {
            "Austin": (30.2672, -97.7431),
            "Dallas": (32.7767, -96.7970),
            "Houston": (29.7604, -95.3698),
            "Atlanta": (33.7490, -84.3880),
            "Phoenix": (33.4484, -112.0740),
            "Denver": (39.7392, -104.9903),
            "Nashville": (36.1627, -86.7816),
            "Charlotte": (35.2271, -80.8431)
        }
        
        base_lat, base_lng = coords.get(location["city"], (30.0, -90.0))
        
        # Add some variance (roughly 10 mile radius)
        lat_variance = random.uniform(-0.1, 0.1)
        lng_variance = random.uniform(-0.1, 0.1)
        
        return {
            "latitude": round(base_lat + lat_variance, 6),
            "longitude": round(base_lng + lng_variance, 6)
        }

    def generate_valuations(self, property_details):
        """Generate property valuations"""
        # Base value calculation
        base_value = (
            property_details["square_feet"] * random.randint(100, 300) +
            property_details["bedrooms"] * 15000 +
            property_details["bathrooms"] * 8000
        )
        
        # Adjust for year built
        age = 2024 - property_details["year_built"]
        age_factor = max(0.7, 1 - (age * 0.01))
        base_value *= age_factor
        
        # Add market variance
        market_variance = random.uniform(0.8, 1.3)
        market_value = int(base_value * market_variance)
        
        return {
            "market_value": market_value,
            "tax_assessment": int(market_value * random.uniform(0.7, 0.9)),
            "insurance_value": int(market_value * random.uniform(1.1, 1.3)),
            "rental_estimate": int(market_value * random.uniform(0.005, 0.012))  # Monthly rent
        }

    def generate_hoa_data(self):
        """Generate HOA data (only for some properties)"""
        if random.random() > 0.4:  # 40% of properties have HOA
            return None
            
        return {
            "hoa_name": random.choice(self.hoa_names),
            "hoa_monthly_fee": random.randint(50, 500),
            "hoa_special_assessment": random.randint(0, 5000) if random.random() > 0.8 else 0,
            "hoa_amenities": random.choice([
                "Pool, Gym, Tennis Court",
                "Playground, Clubhouse",
                "Pool, Spa",
                "Tennis Court, Walking Trails",
                "Clubhouse, Pool, Gym"
            ]),
            "hoa_management": random.choice([
                "ABC Property Management", "XYZ HOA Services", 
                "Community First Management", "Premier HOA Solutions"
            ])
        }

    def generate_rehab_estimates(self):
        """Generate rehabilitation cost estimates"""
        estimates = {}
        
        # Randomly include various rehab categories
        possible_rehab = {
            "kitchen_rehab": (5000, 25000),
            "bathroom_rehab": (3000, 15000),
            "flooring_cost": (2000, 12000),
            "roof_repair": (8000, 20000),
            "hvac_cost": (4000, 12000),
            "electrical_work": (2000, 8000),
            "plumbing_work": (1500, 6000),
            "exterior_paint": (2000, 8000),
            "interior_paint": (1000, 5000)
        }
        
        # Randomly select 2-5 rehab items
        num_items = random.randint(2, 5)
        selected_items = random.sample(list(possible_rehab.keys()), num_items)
        
        for item in selected_items:
            min_cost, max_cost = possible_rehab[item]
            estimates[item] = random.randint(min_cost, max_cost)
        
        return estimates

    def generate_property(self):
        """Generate a complete property record"""
        location = random.choice(self.cities)
        
        property_data = {}
        
        # Basic address and location
        property_data.update(self.generate_address(location))
        property_data.update(self.generate_coordinates(location))
        
        # Property details
        details = self.generate_property_details()
        property_data.update(details)
        
        # Valuations
        valuations = self.generate_valuations(details)
        property_data.update(valuations)
        
        # HOA data (optional)
        hoa_data = self.generate_hoa_data()
        if hoa_data:
            property_data.update(hoa_data)
        
        # Rehab estimates
        rehab_data = self.generate_rehab_estimates()
        property_data.update(rehab_data)
        
        # Additional metadata
        property_data.update({
            "data_source": "Sample Generator",
            "created_date": datetime.now().isoformat(),
            "property_id": f"PROP_{random.randint(100000, 999999)}"
        })
        
        return property_data

    def generate_dataset(self, count=100):
        """Generate a dataset of properties"""
        properties = []
        
        print(f"Generating {count} sample properties...")
        
        for i in range(count):
            if (i + 1) % 10 == 0:
                print(f"Generated {i + 1}/{count} properties")
            
            property_record = self.generate_property()
            properties.append(property_record)
        
        return properties

def main():
    parser = argparse.ArgumentParser(description='Generate sample property data')
    parser.add_argument('--count', type=int, default=100, 
                       help='Number of properties to generate (default: 100)')
    parser.add_argument('--output', type=str, default='data/sample_properties.json',
                       help='Output file path (default: data/sample_properties.json)')
    
    args = parser.parse_args()
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    # Generate data
    generator = PropertyDataGenerator()
    properties = generator.generate_dataset(args.count)
    
    # Save to file
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(properties, f, indent=2, ensure_ascii=False)
    
    print(f"\nSample data generated successfully!")
    print(f"File: {args.output}")
    print(f"Properties: {len(properties)}")
    print(f"File size: {os.path.getsize(args.output) / 1024:.1f} KB")
    
    # Display sample record
    print(f"\nSample property record:")
    print(json.dumps(properties[0], indent=2))

if __name__ == "__main__":
    main()