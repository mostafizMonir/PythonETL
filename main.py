#!/usr/bin/env python3
"""
Main ETL Transfer Script
This script transfers data from source PostgreSQL RDS to target warehouse RDS
"""

import argparse
import sys
import schedule
import time
from datetime import datetime
import logging

from etl_transfer import ETLTransfer
from config import Config

def setup_logging():
    """Setup basic logging for the main script"""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(Config.LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_transfer(drop_target_if_exists: bool = False, incremental: bool = False, 
                date_column: str = None) -> bool:
    """
    Run the ETL transfer process
    
    Args:
        drop_target_if_exists: Whether to drop target table if it exists
        incremental: Whether to perform incremental transfer
        date_column: Column name for incremental transfer
    
    Returns:
        bool: True if transfer was successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("Starting ETL Transfer Process")
        logger.info(f"Source Table: {Config.SOURCE_TABLE}")
        logger.info(f"Target Table: {Config.TARGET_TABLE}")
        logger.info(f"Batch Size: {Config.BATCH_SIZE:,}")
        logger.info(f"Max Workers: {Config.MAX_WORKERS}")
        logger.info(f"Table Splitting: {'Enabled' if Config.ENABLE_TABLE_SPLITTING else 'Disabled'}")
        if Config.ENABLE_TABLE_SPLITTING:
            logger.info(f"Number of Splits: {Config.NUMBER_OF_SPLITS}")
            logger.info(f"ETL Internal Schema: {Config.ETL_INTERNAL_SCHEMA}")
        logger.info("=" * 60)
        
        # Initialize ETL transfer
        etl = ETLTransfer()
        
        # Run transfer based on type
        if incremental and date_column:
            logger.info(f"Running incremental transfer using column: {date_column}")
            result = etl.incremental_transfer(date_column)
        else:
            logger.info("Running full transfer")
            result = etl.transfer_data(drop_target_if_exists)
        
        # Log results
        if result['success']:
            logger.info("✅ Transfer completed successfully!")
            logger.info(f"Rows transferred: {result.get('rows_transferred', 0):,}")
            logger.info(f"Duration: {result.get('duration', 0):.2f} seconds")
            logger.info(f"Transfer rate: {result.get('transfer_rate', 0):.2f} rows/second")
            
            if result.get('batches_processed'):
                logger.info(f"Batches processed: {result['batches_processed']}")
            
            if result.get('batches_failed', 0) > 0:
                logger.warning(f"⚠️  {result['batches_failed']} batches failed")
            
            return True
        else:
            logger.error("❌ Transfer failed!")
            if 'error' in result:
                logger.error(f"Error: {result['error']}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Transfer process failed with exception: {e}")
        return False

def run_scheduled_transfer():
    """Function to run scheduled transfer"""
    logger = logging.getLogger(__name__)
    logger.info(f"Scheduled transfer started at {datetime.now()}")
    
    success = run_transfer(drop_target_if_exists=False)
    
    if success:
        logger.info("Scheduled transfer completed successfully")
    else:
        logger.error("Scheduled transfer failed")
    
    return success

def main():
    """Main function to handle command line arguments and run transfer"""
    parser = argparse.ArgumentParser(description='ETL Transfer Tool for PostgreSQL RDS')
    
    parser.add_argument(
        '--drop-target',
        action='store_true',
        help='Drop target table if it exists before transfer'
    )
    
    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Perform incremental transfer'
    )
    
    parser.add_argument(
        '--date-column',
        type=str,
        help='Date column name for incremental transfer'
    )
    
    parser.add_argument(
        '--schedule',
        type=str,
        help='Schedule time (e.g., "14:30" for daily at 2:30 PM)'
    )
    
    parser.add_argument(
        '--run-now',
        action='store_true',
        help='Run transfer immediately (default behavior)'
    )
    
    parser.add_argument(
        '--enable-splitting',
        action='store_true',
        help='Enable table splitting for large datasets'
    )
    
    parser.add_argument(
        '--disable-splitting',
        action='store_true',
        help='Disable table splitting (use traditional batch processing)'
    )
    
    parser.add_argument(
        '--splits',
        type=int,
        help='Number of temporary tables to create for splitting (default: 10)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Validate arguments
    if args.incremental and not args.date_column:
        logger.error("Date column is required for incremental transfer")
        sys.exit(1)
    
    if args.schedule and args.run_now:
        logger.error("Cannot use both --schedule and --run-now")
        sys.exit(1)
    
    if args.enable_splitting and args.disable_splitting:
        logger.error("Cannot use both --enable-splitting and --disable-splitting")
        sys.exit(1)
    
    # Override configuration based on command line arguments
    if args.enable_splitting:
        Config.ENABLE_TABLE_SPLITTING = True
        logger.info("Table splitting enabled via command line")
    
    if args.disable_splitting:
        Config.ENABLE_TABLE_SPLITTING = False
        logger.info("Table splitting disabled via command line")
    
    if args.splits:
        Config.NUMBER_OF_SPLITS = args.splits
        logger.info(f"Number of splits set to {args.splits} via command line")
    
    # Run based on arguments
    if args.schedule:
        logger.info(f"Scheduling daily transfer at {args.schedule}")
        schedule.every().day.at(args.schedule).do(run_scheduled_transfer)
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
    
    else:
        # Run transfer immediately
        success = run_transfer(
            drop_target_if_exists=args.drop_target,
            incremental=args.incremental,
            date_column=args.date_column
        )
        
        if success:
            logger.info("Transfer completed successfully")
            sys.exit(0)
        else:
            logger.error("Transfer failed")
            sys.exit(1)

if __name__ == "__main__":
    main() 