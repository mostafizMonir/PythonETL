#!/usr/bin/env python3
"""
Test script for table splitting functionality
This script demonstrates how the table splitting feature works
"""

import os
import sys
import logging
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from etl_transfer import ETLTransfer

def setup_test_logging():
    """Setup logging for test script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def test_table_splitting():
    """Test the table splitting functionality"""
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("Testing Table Splitting Functionality")
    logger.info("=" * 60)
    
    try:
        # Initialize ETL transfer
        etl = ETLTransfer()
        
        # Test 1: Validate connections
        logger.info("Test 1: Validating database connections...")
        if not etl.validate_connections():
            logger.error("❌ Database connection validation failed")
            return False
        logger.info("✅ Database connections validated successfully")
        
        # Test 2: Get source table info
        logger.info("Test 2: Getting source table information...")
        total_rows = etl.get_total_rows()
        logger.info(f"✅ Source table has {total_rows:,} rows")
        
        # Test 3: Test table splitting calculation
        logger.info("Test 3: Testing table splitting calculation...")
        rows_per_split = total_rows // Config.NUMBER_OF_SPLITS
        remaining_rows = total_rows % Config.NUMBER_OF_SPLITS
        
        logger.info(f"Number of splits: {Config.NUMBER_OF_SPLITS}")
        logger.info(f"Rows per split: {rows_per_split:,}")
        logger.info(f"Remaining rows: {remaining_rows:,}")
        
        # Calculate expected temporary table names
        expected_temp_tables = []
        for i in range(Config.NUMBER_OF_SPLITS):
            expected_temp_tables.append(f"{Config.SOURCE_TABLE}_{i + 1}")
        
        logger.info(f"Expected temporary tables: {expected_temp_tables}")
        logger.info("✅ Table splitting calculation completed")
        
        # Test 4: Check if ETL internal schema exists or can be created
        logger.info("Test 4: Testing ETL internal schema creation...")
        try:
            etl.source_db.create_schema_if_not_exists(Config.ETL_INTERNAL_SCHEMA)
            logger.info(f"✅ ETL internal schema '{Config.ETL_INTERNAL_SCHEMA}' ready")
        except Exception as e:
            logger.error(f"❌ Failed to create ETL internal schema: {e}")
            return False
        
        # Test 5: Check existing temporary tables (cleanup if any)
        logger.info("Test 5: Checking for existing temporary tables...")
        existing_tables = etl.source_db.get_table_names_in_schema(
            Config.ETL_INTERNAL_SCHEMA, 
            f"{Config.SOURCE_TABLE}_%"
        )
        
        if existing_tables:
            logger.warning(f"Found {len(existing_tables)} existing temporary tables, cleaning up...")
            for table_name in existing_tables:
                etl.source_db.drop_table(table_name, Config.ETL_INTERNAL_SCHEMA)
            logger.info("✅ Cleaned up existing temporary tables")
        else:
            logger.info("✅ No existing temporary tables found")
        
        # Test 6: Test creating a single temporary table (small subset)
        logger.info("Test 6: Testing temporary table creation...")
        test_temp_table = f"{Config.SOURCE_TABLE}_test"
        test_limit = min(1000, total_rows)  # Test with max 1000 rows
        
        success = etl.source_db.create_temp_table_from_source(
            Config.SOURCE_TABLE,
            Config.SOURCE_SCHEMA,
            test_temp_table,
            Config.ETL_INTERNAL_SCHEMA,
            0,  # offset
            test_limit
        )
        
        if success:
            test_rows = etl.source_db.get_row_count(test_temp_table, Config.ETL_INTERNAL_SCHEMA)
            logger.info(f"✅ Created test temporary table with {test_rows:,} rows")
            
            # Clean up test table
            etl.source_db.drop_table(test_temp_table, Config.ETL_INTERNAL_SCHEMA)
            logger.info("✅ Cleaned up test temporary table")
        else:
            logger.error("❌ Failed to create test temporary table")
            return False
        
        logger.info("=" * 60)
        logger.info("✅ All table splitting tests passed!")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed with exception: {e}")
        return False

def main():
    """Main function"""
    setup_test_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting table splitting functionality test...")
    
    success = test_table_splitting()
    
    if success:
        logger.info("🎉 All tests completed successfully!")
        logger.info("The table splitting functionality is ready to use.")
        logger.info("You can now run the main ETL transfer with table splitting enabled.")
        sys.exit(0)
    else:
        logger.error("💥 Tests failed. Please check the configuration and database connections.")
        sys.exit(1)

if __name__ == "__main__":
    main() 