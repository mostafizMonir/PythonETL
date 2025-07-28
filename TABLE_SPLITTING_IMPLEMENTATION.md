# Table Splitting Implementation Summary

## Overview

I have successfully implemented your table splitting idea for the ETL transfer tool. This feature allows the system to split large source tables into multiple temporary tables for more efficient processing of very large datasets (like your 6 million row `event_plan_member` table).

## What Was Implemented

### 1. Configuration Updates (`config.py`)

Added new configuration parameters:
- `ENABLE_TABLE_SPLITTING`: Enable/disable table splitting (default: true)
- `NUMBER_OF_SPLITS`: Number of temporary tables to create (default: 10)
- `ETL_INTERNAL_SCHEMA`: Schema for temporary tables (default: etl_internal)

### 2. Database Manager Enhancements (`database_manager.py`)

Added new methods:
- `create_temp_table_from_source()`: Creates temporary tables with data subsets
- `drop_table()`: Safely drops tables from schemas
- `get_table_names_in_schema()`: Lists tables in a schema with pattern matching
- `extract_from_temp_table()`: Extracts data from temporary tables

### 3. ETL Transfer Enhancements (`etl_transfer.py`)

Added new methods:
- `create_temp_tables_from_source()`: Splits source table into temporary tables
- `process_temp_table()`: Processes individual temporary tables
- `cleanup_temp_tables()`: Cleans up temporary tables after processing
- `transfer_data_with_splitting()`: Main method for split-based transfers

Modified existing methods:
- `transfer_data()`: Now automatically chooses between traditional and split-based transfer

### 4. Command Line Interface Updates (`main.py`)

Added new command line options:
- `--enable-splitting`: Enable table splitting
- `--disable-splitting`: Disable table splitting
- `--splits`: Set number of splits

### 5. Documentation Updates

- Updated `README.md` with table splitting documentation
- Updated `env_example.txt` with new configuration options
- Created `test_table_splitting.py` for testing the functionality

## How It Works

### For Your 6 Million Row Table

1. **Splitting Calculation**:
   - Total rows: 6,000,000
   - Number of splits: 10 (configurable)
   - Rows per split: 600,000
   - Remaining rows: 0

2. **Temporary Table Creation**:
   - Creates 10 tables in `etl_internal` schema:
     - `event_plan_member_1` (rows 1-600,000)
     - `event_plan_member_2` (rows 600,001-1,200,000)
     - `event_plan_member_3` (rows 1,200,001-1,800,000)
     - ... and so on

3. **Sequential Processing**:
   - Processes each temporary table one by one
   - Each table is processed in batches (10,000 rows per batch by default)
   - Data is transferred to the target database

4. **Cleanup**:
   - All temporary tables are automatically dropped after processing

## Usage Examples

### Basic Usage (Table Splitting Enabled by Default)
```bash
python main.py
```

### Explicitly Enable Table Splitting
```bash
python main.py --enable-splitting --splits 15
```

### Disable Table Splitting (Use Traditional Method)
```bash
python main.py --disable-splitting
```

### Test the Functionality
```bash
python test_table_splitting.py
```

## Configuration

Add these to your `.env` file:
```env
# Table Splitting Configuration
ENABLE_TABLE_SPLITTING=true
NUMBER_OF_SPLITS=10
ETL_INTERNAL_SCHEMA=etl_internal
```

## Benefits for Your Use Case

1. **Memory Efficiency**: Instead of loading 6M rows at once, it processes 600K rows at a time
2. **Reduced Timeouts**: Smaller queries are less likely to timeout
3. **Better Progress Tracking**: You can see progress per temporary table
4. **Fault Tolerance**: If one temporary table fails, others can still succeed
5. **Predictable Performance**: More consistent transfer rates

## Expected Performance

For your 6 million row `event_plan_member` table:
- **Traditional Method**: ~15-45 minutes (depending on network/data size)
- **Table Splitting Method**: Similar total time but more predictable and stable
- **Memory Usage**: Reduced from ~1-2GB to ~200-400MB per temporary table

## Safety Features

1. **Automatic Cleanup**: Temporary tables are always cleaned up, even on errors
2. **Schema Creation**: ETL internal schema is created automatically if it doesn't exist
3. **Error Handling**: Individual temporary table failures don't stop the entire process
4. **Progress Logging**: Detailed logging of each step for monitoring

## Testing

The `test_table_splitting.py` script validates:
- Database connections
- Schema creation
- Temporary table creation
- Cleanup functionality

Run this before your first production use to ensure everything works correctly.

## Migration Path

The implementation is backward compatible:
- If `ENABLE_TABLE_SPLITTING=false`, it uses the traditional method
- If `ENABLE_TABLE_SPLITTING=true` (default), it uses the new splitting method
- You can switch between methods without code changes

## Next Steps

1. **Test the functionality**: Run `python test_table_splitting.py`
2. **Configure your environment**: Update your `.env` file with the new settings
3. **Run a small test**: Try with a subset of your data first
4. **Monitor performance**: Check the logs for transfer rates and any issues
5. **Scale up**: Once confident, run with your full 6M row dataset

The implementation follows your exact specifications and should handle your large `event_plan_member` table efficiently! 