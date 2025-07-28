import pandas as pd
import logging
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from tqdm import tqdm
import psycopg2
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json
import numpy as np

from config import Config
from database_manager import DatabaseManager

class ETLTransfer:
    def __init__(self):
        """Initialize ETL transfer with source and target database managers"""
        self.source_db = DatabaseManager(
            Config.get_source_connection_string(),
            Config.CONNECTION_TIMEOUT
        )
        self.target_db = DatabaseManager(
            Config.get_target_connection_string(),
            Config.CONNECTION_TIMEOUT
        )
        self.logger = self._setup_logging()
        self.lock = threading.Lock()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('ETLTransfer')
        logger.setLevel(getattr(logging, Config.LOG_LEVEL))
        
        # Create handlers
        file_handler = logging.FileHandler(Config.LOG_FILE)
        console_handler = logging.StreamHandler()
        
        # Create formatters and add it to handlers
        log_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(log_format)
        console_handler.setFormatter(log_format)
        
        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and convert data types for PostgreSQL compatibility"""
        if df.empty:
            return df
        
        df_clean = df.copy()
        
        for column in df_clean.columns:
            # Handle dictionary/JSON objects
            if df_clean[column].dtype == 'object':
                # Check if column contains dictionaries
                dict_mask = df_clean[column].apply(lambda x: isinstance(x, dict) if pd.notna(x) else False)
                if dict_mask.any():
                    # Convert dictionaries to JSON strings
                    df_clean[column] = df_clean[column].apply(
                        lambda x: json.dumps(x) if isinstance(x, dict) and pd.notna(x) else x
                    )
                    self.logger.debug(f"Converted dictionary column '{column}' to JSON strings")
                
                # Handle numpy arrays
                array_mask = df_clean[column].apply(lambda x: isinstance(x, np.ndarray) if pd.notna(x) else False)
                if array_mask.any():
                    # Convert numpy arrays to lists then to JSON
                    df_clean[column] = df_clean[column].apply(
                        lambda x: json.dumps(x.tolist()) if isinstance(x, np.ndarray) and pd.notna(x) else x
                    )
                    self.logger.debug(f"Converted numpy array column '{column}' to JSON strings")
                
                # Handle lists
                list_mask = df_clean[column].apply(lambda x: isinstance(x, list) if pd.notna(x) else False)
                if list_mask.any():
                    # Convert lists to JSON strings
                    df_clean[column] = df_clean[column].apply(
                        lambda x: json.dumps(x) if isinstance(x, list) and pd.notna(x) else x
                    )
                    self.logger.debug(f"Converted list column '{column}' to JSON strings")
            
            # Handle numpy data types
            elif df_clean[column].dtype == np.int64:
                df_clean[column] = df_clean[column].astype('int64')
            elif df_clean[column].dtype == np.float64:
                df_clean[column] = df_clean[column].astype('float64')
            elif df_clean[column].dtype == np.bool_:
                df_clean[column] = df_clean[column].astype('bool')
            
            # Handle datetime with timezone issues
            elif pd.api.types.is_datetime64_any_dtype(df_clean[column]):
                # Convert to timezone-naive datetime if needed
                df_clean[column] = pd.to_datetime(df_clean[column]).dt.tz_localize(None)
        
        return df_clean
    
    def validate_connections(self) -> bool:
        """Validate both source and target database connections"""
        self.logger.info("Validating database connections...")
        
        source_ok = self.source_db.test_connection()
        target_ok = self.target_db.test_connection()
        
        if not source_ok:
            self.logger.error("Source database connection failed")
            return False
        
        if not target_ok:
            self.logger.error("Target database connection failed")
            return False
        
        self.logger.info("Database connections validated successfully")
        return True
    
    def get_source_schema(self) -> List[Dict[str, Any]]:
        """Get source table schema"""
        self.logger.info(f"Getting schema for source table: {Config.SOURCE_TABLE}")
        schema = self.source_db.get_table_schema(Config.SOURCE_TABLE, Config.SOURCE_SCHEMA)
        
        if not schema:
            raise ValueError(f"Could not retrieve schema for table {Config.SOURCE_TABLE}")
        
        self.logger.info(f"Retrieved schema with {len(schema)} columns")
        return schema
    
    def prepare_target_table(self, schema: List[Dict[str, Any]], 
                           drop_if_exists: bool = False):
        """Prepare target table with same schema as source in ETL schema"""
        self.logger.info(f"Preparing target table: {Config.TARGET_SCHEMA}.{Config.TARGET_TABLE}")
        
        try:
            self.target_db.create_table_if_not_exists(
                Config.TARGET_TABLE, 
                Config.TARGET_SCHEMA,
                schema, 
                drop_if_exists
            )
            self.logger.info("Target table prepared successfully")
        except Exception as e:
            self.logger.error(f"Failed to prepare target table: {e}")
            raise
    
    def get_total_rows(self) -> int:
        """Get total number of rows in source table"""
        self.logger.info("Getting total row count from source table...")
        total_rows = self.source_db.get_row_count(Config.SOURCE_TABLE, Config.SOURCE_SCHEMA)
        self.logger.info(f"Total rows in source table: {total_rows:,}")
        return total_rows
    
    def extract_batch(self, offset: int, limit: int) -> pd.DataFrame:
        """Extract a batch of data from source table"""
        query = f"""
        SELECT * FROM {Config.SOURCE_TABLE}
        ORDER BY 1  -- Order by first column for consistent pagination
        LIMIT %s OFFSET %s
        """
        
        try:
            engine = self.source_db.get_engine()
            df = pd.read_sql(
                query, 
                engine, 
                params=(limit, offset),
                chunksize=Config.CHUNK_SIZE
            )
            
            # If chunksize is used, read all chunks
            if hasattr(df, '__iter__'):
                chunks = []
                for chunk in df:
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            
            # Clean the dataframe before returning
            df_clean = self._clean_dataframe(df)
            return df_clean
                
        except Exception as e:
            self.logger.error(f"Failed to extract batch (offset={offset}, limit={limit}): {e}")
            raise
    
    def load_batch(self, df: pd.DataFrame, batch_number: int) -> bool:
        """Load a batch of data to target table in ETL schema"""
        if df.empty:
            self.logger.warning(f"Batch {batch_number} is empty, skipping...")
            return True
        
        try:
            engine = self.target_db.get_engine()
            
            # Use to_sql with schema specification for batch loading
            df.to_sql(
                Config.TARGET_TABLE,
                engine,
                schema=Config.TARGET_SCHEMA,
                if_exists='append',
                index=False,
                method='multi',  # Use multi-row insert for better performance
                chunksize=1000   # Internal chunk size for to_sql
            )
            
            self.logger.debug(f"Successfully loaded batch {batch_number} with {len(df):,} rows to {Config.TARGET_SCHEMA}.{Config.TARGET_TABLE}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load batch {batch_number}: {e}")
            # Log more details about the problematic data
            self.logger.error(f"DataFrame info for batch {batch_number}:")
            self.logger.error(f"Shape: {df.shape}")
            self.logger.error(f"Columns: {list(df.columns)}")
            self.logger.error(f"Data types: {df.dtypes.to_dict()}")
            
            # Check for problematic data types
            for col in df.columns:
                if df[col].dtype == 'object':
                    sample_values = df[col].dropna().head(3)
                    self.logger.error(f"Sample values in column '{col}': {sample_values.tolist()}")
            
            return False
    
    def process_batch(self, batch_number: int, offset: int, limit: int) -> Dict[str, Any]:
        """Process a single batch (extract and load)"""
        start_time = time.time()
        
        try:
            # Extract batch
            self.logger.debug(f"Extracting batch {batch_number} (offset={offset}, limit={limit})")
            df = self.extract_batch(offset, limit)
            
            if df.empty:
                return {
                    'batch_number': batch_number,
                    'rows_processed': 0,
                    'success': True,
                    'duration': time.time() - start_time,
                    'error': None
                }
            
            # Load batch
            self.logger.debug(f"Loading batch {batch_number} with {len(df):,} rows")
            success = self.load_batch(df, batch_number)
            
            return {
                'batch_number': batch_number,
                'rows_processed': len(df),
                'success': success,
                'duration': time.time() - start_time,
                'error': None if success else "Load failed"
            }
            
        except Exception as e:
            self.logger.error(f"Batch {batch_number} processing failed: {e}")
            return {
                'batch_number': batch_number,
                'rows_processed': 0,
                'success': False,
                'duration': time.time() - start_time,
                'error': str(e)
            }
    
    def transfer_data(self, drop_target_if_exists: bool = False) -> Dict[str, Any]:
        """Main method to transfer data from source to target"""
        # Check if table splitting is enabled
        if Config.ENABLE_TABLE_SPLITTING:
            self.logger.info("Table splitting is enabled, using split-based transfer")
            return self.transfer_data_with_splitting(drop_target_if_exists)
        else:
            self.logger.info("Table splitting is disabled, using traditional batch transfer")
            return self._transfer_data_traditional(drop_target_if_exists)
    
    def _transfer_data_traditional(self, drop_target_if_exists: bool = False) -> Dict[str, Any]:
        """Traditional batch-based transfer method"""
        start_time = time.time()
        
        try:
            # Step 1: Validate connections
            if not self.validate_connections():
                raise Exception("Database connection validation failed")
            
            # Step 2: Get source schema
            schema = self.get_source_schema()
            
            # Step 3: Prepare target table
            if drop_target_if_exists:
                self.prepare_target_table(schema, drop_if_exists=True)
            else:
                # If table exists, truncate it first                
                self.prepare_target_table(schema, drop_if_exists=False)
                self.target_db.truncate_table(Config.TARGET_TABLE, Config.TARGET_SCHEMA)
            
            # Step 4: Get total rows
            total_rows = self.get_total_rows()
            
            if total_rows == 0:
                self.logger.warning("Source table is empty, nothing to transfer")
                return {
                    'success': True,
                    'rows_transferred': 0,
                    'duration': time.time() - start_time,
                    'batches_processed': 0
                }
            
            # Step 5: Calculate batches
            total_batches = (total_rows + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
            self.logger.info(f"Will process {total_batches} batches of {Config.BATCH_SIZE:,} rows each")
            
            # Step 6: Process batches
            processed_rows = 0
            successful_batches = 0
            failed_batches = 0
            
            # Create progress bar
            pbar = tqdm(total=total_batches, desc="Processing batches")
            
            # Process batches with ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
                # Submit all batch tasks
                future_to_batch = {}
                for batch_num in range(total_batches):
                    offset = batch_num * Config.BATCH_SIZE
                    limit = min(Config.BATCH_SIZE, total_rows - offset)
                    
                    future = executor.submit(self.process_batch, batch_num + 1, offset, limit)
                    future_to_batch[future] = batch_num + 1
                
                # Process completed batches
                for future in as_completed(future_to_batch):
                    batch_num = future_to_batch[future]
                    result = future.result()
                    
                    with self.lock:
                        if result['success']:
                            processed_rows += result['rows_processed']
                            successful_batches += 1
                        else:
                            failed_batches += 1
                            self.logger.error(f"Batch {batch_num} failed: {result['error']}")
                        
                        pbar.update(1)
                        pbar.set_postfix({
                            'Processed': f"{processed_rows:,}",
                            'Success': successful_batches,
                            'Failed': failed_batches
                        })
            
            pbar.close()
            
            # Step 7: Final validation
            target_rows = self.target_db.get_row_count(Config.TARGET_TABLE, Config.TARGET_SCHEMA)
            
            duration = time.time() - start_time
            
            result = {
                'success': failed_batches == 0,
                'rows_transferred': processed_rows,
                'target_rows': target_rows,
                'duration': duration,
                'batches_processed': successful_batches,
                'batches_failed': failed_batches,
                'transfer_rate': processed_rows / duration if duration > 0 else 0
            }
            
            self.logger.info(f"Transfer completed in {duration:.2f} seconds")
            self.logger.info(f"Rows transferred: {processed_rows:,}")
            self.logger.info(f"Target table rows: {target_rows:,}")
            self.logger.info(f"Transfer rate: {result['transfer_rate']:.2f} rows/second")
            
            if failed_batches > 0:
                self.logger.warning(f"{failed_batches} batches failed during transfer")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Transfer failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'duration': time.time() - start_time
            }
    
    def incremental_transfer(self, date_column: str, last_transfer_date: Optional[str] = None) -> Dict[str, Any]:
        """Perform incremental transfer based on date column"""
        self.logger.info(f"Starting incremental transfer using column: {date_column}")
        
        # Build where clause for incremental transfer
        if last_transfer_date:
            where_clause = f"{date_column} > '{last_transfer_date}'"
        else:
            # If no last transfer date, transfer last 24 hours
            where_clause = f"{date_column} >= CURRENT_DATE - INTERVAL '1 day'"
        
        # Get count of new records
        new_rows = self.source_db.get_row_count(Config.SOURCE_TABLE, where_clause=where_clause)
        self.logger.info(f"Found {new_rows:,} new rows to transfer")
        
        if new_rows == 0:
            self.logger.info("No new rows to transfer")
            return {
                'success': True,
                'rows_transferred': 0,
                'duration': 0,
                'batches_processed': 0
            }
        
        # Modify the transfer logic for incremental transfer
        # This is a simplified version - you might want to implement a more sophisticated approach
        return self.transfer_data(drop_target_if_exists=False)
    
    def create_temp_tables_from_source(self) -> List[str]:
        """
        Split source table into multiple temporary tables
        
        Returns:
            List[str]: List of temporary table names created
        """
        self.logger.info("Starting table splitting process...")
        
        # Get total rows from source table
        total_rows = self.get_total_rows()
        if total_rows == 0:
            self.logger.warning("Source table is empty, no temporary tables to create")
            return []
        
        # Calculate rows per split
        rows_per_split = total_rows // Config.NUMBER_OF_SPLITS
        remaining_rows = total_rows % Config.NUMBER_OF_SPLITS
        
        self.logger.info(f"Total rows: {total_rows:,}")
        self.logger.info(f"Number of splits: {Config.NUMBER_OF_SPLITS}")
        self.logger.info(f"Rows per split: {rows_per_split:,}")
        self.logger.info(f"Remaining rows: {remaining_rows:,}")
        
        temp_table_names = []
        current_offset = 0
        
        # Create temporary tables
        for split_num in range(Config.NUMBER_OF_SPLITS):
            temp_table_name = f"{Config.SOURCE_TABLE}_{split_num + 1}"
            temp_table_names.append(temp_table_name)
            
            # Calculate limit for this split
            if split_num < remaining_rows:
                # Distribute remaining rows among first splits
                limit = rows_per_split + 1
            else:
                limit = rows_per_split
            
            self.logger.info(f"Creating temporary table {temp_table_name} (offset={current_offset:,}, limit={limit:,})")
            
            # Create temporary table
            success = self.source_db.create_temp_table_from_source(
                Config.SOURCE_TABLE,
                Config.SOURCE_SCHEMA,
                temp_table_name,
                Config.ETL_INTERNAL_SCHEMA,
                current_offset,
                limit
            )
            
            if not success:
                self.logger.error(f"Failed to create temporary table {temp_table_name}")
                # Clean up already created tables
                self.cleanup_temp_tables(temp_table_names)
                raise Exception(f"Failed to create temporary table {temp_table_name}")
            
            current_offset += limit
        
        self.logger.info(f"Successfully created {len(temp_table_names)} temporary tables")
        return temp_table_names
    
    def process_temp_table_batch(self, temp_table_name: str, batch_number: int, offset: int, limit: int) -> Dict[str, Any]:
        """
        Process a single batch from a temporary table
        
        Args:
            temp_table_name: Name of the temporary table
            batch_number: Batch number for logging
            offset: Offset for data extraction
            limit: Limit for data extraction
            
        Returns:
            Dict[str, Any]: Batch processing results
        """
        start_time = time.time()
        
        try:
            # Extract from temporary table
            df = self.source_db.extract_from_temp_table(
                temp_table_name,
                Config.ETL_INTERNAL_SCHEMA,
                offset,
                limit
            )
            
            if df.empty:
                return {
                    'temp_table_name': temp_table_name,
                    'batch_number': batch_number,
                    'rows_processed': 0,
                    'success': True,
                    'duration': time.time() - start_time,
                    'error': None
                }
            
            # Clean the dataframe
            df_clean = self._clean_dataframe(df)
            
            # Load to target
            success = self.load_batch(df_clean, f"{temp_table_name}_batch_{batch_number}")
            
            return {
                'temp_table_name': temp_table_name,
                'batch_number': batch_number,
                'rows_processed': len(df_clean),
                'success': success,
                'duration': time.time() - start_time,
                'error': None if success else "Load failed"
            }
            
        except Exception as e:
            self.logger.error(f"Batch {batch_number} processing failed for {temp_table_name}: {e}")
            return {
                'temp_table_name': temp_table_name,
                'batch_number': batch_number,
                'rows_processed': 0,
                'success': False,
                'duration': time.time() - start_time,
                'error': str(e)
            }

    def process_temp_table(self, temp_table_name: str, temp_table_index: int) -> Dict[str, Any]:
        """
        Process a single temporary table with multi-threading
        
        Args:
            temp_table_name: Name of the temporary table
            temp_table_index: Index of the temporary table (for logging)
            
        Returns:
            Dict[str, Any]: Processing results
        """
        start_time = time.time()
        
        try:
            self.logger.info(f"Processing temporary table {temp_table_index + 1}: {temp_table_name}")
            
            # Get row count for this temporary table
            temp_table_rows = self.source_db.get_row_count(temp_table_name, Config.ETL_INTERNAL_SCHEMA)
            self.logger.info(f"Temporary table {temp_table_name} has {temp_table_rows:,} rows")
            
            if temp_table_rows == 0:
                return {
                    'temp_table_name': temp_table_name,
                    'temp_table_index': temp_table_index,
                    'rows_processed': 0,
                    'success': True,
                    'duration': time.time() - start_time,
                    'error': None
                }
            
            # Calculate batches for this temporary table
            total_batches = (temp_table_rows + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
            self.logger.info(f"Will process {total_batches} batches for {temp_table_name}")
            
            processed_rows = 0
            successful_batches = 0
            failed_batches = 0
            
            # Process batches with ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
                # Submit all batch tasks
                future_to_batch = {}
                for batch_num in range(total_batches):
                    offset = batch_num * Config.BATCH_SIZE
                    limit = min(Config.BATCH_SIZE, temp_table_rows - offset)
                    
                    future = executor.submit(
                        self.process_temp_table_batch, 
                        temp_table_name, 
                        batch_num + 1, 
                        offset, 
                        limit
                    )
                    future_to_batch[future] = batch_num + 1
                
                # Process completed batches
                for future in as_completed(future_to_batch):
                    batch_num = future_to_batch[future]
                    result = future.result()
                    
                    with self.lock:
                        if result['success']:
                            processed_rows += result['rows_processed']
                            successful_batches += 1
                        else:
                            failed_batches += 1
                            self.logger.error(f"Batch {batch_num} failed for {temp_table_name}: {result['error']}")
            
            duration = time.time() - start_time
            
            result = {
                'temp_table_name': temp_table_name,
                'temp_table_index': temp_table_index,
                'rows_processed': processed_rows,
                'success': failed_batches == 0,
                'duration': duration,
                'batches_processed': successful_batches,
                'batches_failed': failed_batches,
                'error': None if failed_batches == 0 else f"{failed_batches} batches failed"
            }
            
            self.logger.info(f"Completed processing {temp_table_name}: {processed_rows:,} rows in {duration:.2f}s")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to process temporary table {temp_table_name}: {e}")
            return {
                'temp_table_name': temp_table_name,
                'temp_table_index': temp_table_index,
                'rows_processed': 0,
                'success': False,
                'duration': time.time() - start_time,
                'error': str(e)
            }
    
    def cleanup_temp_tables(self, temp_table_names: List[str]) -> bool:
        """
        Clean up temporary tables
        
        Args:
            temp_table_names: List of temporary table names to drop
            
        Returns:
            bool: True if all tables were dropped successfully
        """
        self.logger.info(f"Cleaning up {len(temp_table_names)} temporary tables...")
        
        success_count = 0
        for temp_table_name in temp_table_names:
            if self.source_db.drop_table(temp_table_name, Config.ETL_INTERNAL_SCHEMA):
                success_count += 1
            else:
                self.logger.warning(f"Failed to drop temporary table {temp_table_name}")
        
        if success_count == len(temp_table_names):
            self.logger.info("All temporary tables cleaned up successfully")
            return True
        else:
            self.logger.warning(f"Cleaned up {success_count}/{len(temp_table_names)} temporary tables")
            return False
    
    def transfer_data_with_splitting(self, drop_target_if_exists: bool = False) -> Dict[str, Any]:
        """
        Transfer data using table splitting approach
        
        Args:
            drop_target_if_exists: Whether to drop target table if it exists
            
        Returns:
            Dict[str, Any]: Transfer results
        """
        start_time = time.time()
        temp_table_names = []
        
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting ETL Transfer with Table Splitting")
            self.logger.info(f"Source Table: {Config.SOURCE_SCHEMA}.{Config.SOURCE_TABLE}")
            self.logger.info(f"Target Table: {Config.TARGET_SCHEMA}.{Config.TARGET_TABLE}")
            self.logger.info(f"Number of Splits: {Config.NUMBER_OF_SPLITS}")
            self.logger.info(f"ETL Internal Schema: {Config.ETL_INTERNAL_SCHEMA}")
            self.logger.info("=" * 60)
            
            # Step 1: Validate connections
            if not self.validate_connections():
                raise Exception("Database connection validation failed")
            
            # Step 2: Get source schema
            schema = self.get_source_schema()
            
            # Step 3: Prepare target table
            if drop_target_if_exists:
                self.prepare_target_table(schema, drop_if_exists=True)
            else:
                self.prepare_target_table(schema, drop_if_exists=False)
                self.target_db.truncate_table(Config.TARGET_TABLE, Config.TARGET_SCHEMA)
            
            # Step 4: Create temporary tables
            temp_table_names = self.create_temp_tables_from_source()
            
            if not temp_table_names:
                self.logger.warning("No temporary tables created, nothing to transfer")
                return {
                    'success': True,
                    'rows_transferred': 0,
                    'duration': time.time() - start_time,
                    'temp_tables_processed': 0
                }
            
            # Step 5: Process each temporary table
            total_rows_transferred = 0
            successful_temp_tables = 0
            failed_temp_tables = 0
            
            # Create progress bar for temporary tables
            pbar = tqdm(total=len(temp_table_names), desc="Processing temp tables")
            
            for temp_table_index, temp_table_name in enumerate(temp_table_names):
                result = self.process_temp_table(temp_table_name, temp_table_index)
                
                if result['success']:
                    total_rows_transferred += result['rows_processed']
                    successful_temp_tables += 1
                else:
                    failed_temp_tables += 1
                    self.logger.error(f"Temporary table {temp_table_name} failed: {result['error']}")
                
                pbar.update(1)
                pbar.set_postfix({
                    'Processed': f"{total_rows_transferred:,}",
                    'Success': successful_temp_tables,
                    'Failed': failed_temp_tables
                })
            
            pbar.close()
            
            # Step 6: Clean up temporary tables
            self.cleanup_temp_tables(temp_table_names)
            
            # Step 7: Final validation
            target_rows = self.target_db.get_row_count(Config.TARGET_TABLE, Config.TARGET_SCHEMA)
            
            duration = time.time() - start_time
            
            result = {
                'success': failed_temp_tables == 0,
                'rows_transferred': total_rows_transferred,
                'target_rows': target_rows,
                'duration': duration,
                'temp_tables_processed': successful_temp_tables,
                'temp_tables_failed': failed_temp_tables,
                'transfer_rate': total_rows_transferred / duration if duration > 0 else 0
            }
            
            self.logger.info(f"Transfer with splitting completed in {duration:.2f} seconds")
            self.logger.info(f"Rows transferred: {total_rows_transferred:,}")
            self.logger.info(f"Target table rows: {target_rows:,}")
            self.logger.info(f"Transfer rate: {result['transfer_rate']:.2f} rows/second")
            
            if failed_temp_tables > 0:
                self.logger.warning(f"{failed_temp_tables} temporary tables failed during transfer")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Transfer with splitting failed: {e}")
            
            # Clean up temporary tables on error
            if temp_table_names:
                self.cleanup_temp_tables(temp_table_names)
            
            return {
                'success': False,
                'error': str(e),
                'duration': time.time() - start_time
            } 