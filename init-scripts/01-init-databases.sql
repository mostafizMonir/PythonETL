-- Initialize source database with sample data for testing
-- This script creates a sample table with 5 million rows

-- Create a sample table structure
CREATE TABLE IF NOT EXISTS source_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    age INTEGER,
    city VARCHAR(100),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active',
    score DECIMAL(10,2),
    description TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_source_table_created_at ON source_table(created_at);
CREATE INDEX IF NOT EXISTS idx_source_table_status ON source_table(status);
CREATE INDEX IF NOT EXISTS idx_source_table_city ON source_table(city);

-- Function to generate sample data
CREATE OR REPLACE FUNCTION generate_sample_data(num_rows INTEGER)
RETURNS VOID AS $$
DECLARE
    i INTEGER;
    cities TEXT[] := ARRAY['New York', 'London', 'Tokyo', 'Paris', 'Berlin', 'Sydney', 'Toronto', 'Mumbai', 'Singapore', 'Dubai'];
    countries TEXT[] := ARRAY['USA', 'UK', 'Japan', 'France', 'Germany', 'Australia', 'Canada', 'India', 'Singapore', 'UAE'];
    statuses TEXT[] := ARRAY['active', 'inactive', 'pending', 'suspended'];
BEGIN
    -- Generate sample data in batches
    FOR i IN 1..num_rows LOOP
        INSERT INTO source_table (
            name,
            email,
            age,
            city,
            country,
            created_at,
            updated_at,
            status,
            score,
            description
        ) VALUES (
            'User ' || i,
            'user' || i || '@example.com',
            (random() * 80 + 18)::INTEGER,
            cities[1 + (i % array_length(cities, 1))],
            countries[1 + (i % array_length(countries, 1))],
            CURRENT_TIMESTAMP - (random() * interval '365 days'),
            CURRENT_TIMESTAMP - (random() * interval '30 days'),
            statuses[1 + (i % array_length(statuses, 1))],
            (random() * 100)::DECIMAL(10,2),
            'Sample description for user ' || i
        );
        
        -- Commit every 1000 rows to avoid long transactions
        IF i % 1000 = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- Generate 5 million sample rows (this will take some time)
-- Uncomment the line below to generate sample data
-- SELECT generate_sample_data(5000000);

-- For faster testing, generate a smaller dataset first
SELECT generate_sample_data(10000);

-- Display table info
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats 
WHERE tablename = 'source_table';

-- Show row count
SELECT COUNT(*) as total_rows FROM source_table; 