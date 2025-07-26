# PostgreSQL ETL Transfer Tool

A robust Python-based ETL (Extract, Transform, Load) tool designed to transfer large datasets (5+ million rows) between PostgreSQL RDS instances efficiently. This tool is specifically optimized for AWS RDS environments and includes features like batch processing, parallel execution, progress tracking, and scheduling capabilities.

## Features

- **Large Dataset Handling**: Optimized for transferring 5+ million rows efficiently
- **Batch Processing**: Configurable batch sizes to manage memory usage
- **Parallel Execution**: Multi-threaded processing for faster transfers
- **Progress Tracking**: Real-time progress bars and detailed logging
- **Connection Pooling**: Efficient database connection management
- **Error Handling**: Robust error handling with retry mechanisms
- **Scheduling**: Built-in scheduling for daily automated transfers
- **Incremental Transfers**: Support for incremental data transfers
- **Schema Management**: Automatic table creation and schema replication

## Prerequisites

- Python 3.8 or higher
- Access to source PostgreSQL RDS instance
- Access to target PostgreSQL RDS instance (warehouse)
- Network connectivity between your machine and both RDS instances

## Installation

1. **Clone or download the project files**

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**:
   ```bash
   # Copy the example environment file
   cp env_example.txt .env
   
   # Edit .env with your actual database credentials
   nano .env
   ```

## Configuration

Edit the `.env` file with your actual database credentials and settings:

```env
# Source Database (where data is extracted from)
SOURCE_HOST=your-source-rds-endpoint.region.rds.amazonaws.com
SOURCE_PORT=5432
SOURCE_DATABASE=your_source_db
SOURCE_USERNAME=your_username
SOURCE_PASSWORD=your_password

# Target Database (warehouse - where data is loaded to)
TARGET_HOST=your-warehouse-rds-endpoint.region.rds.amazonaws.com
TARGET_PORT=5432
TARGET_DATABASE=your_warehouse_db
TARGET_USERNAME=your_username
TARGET_PASSWORD=your_password

# ETL Configuration
BATCH_SIZE=10000          # Rows per batch (adjust based on memory)
MAX_WORKERS=4             # Number of parallel workers
CHUNK_SIZE=50000          # Pandas chunk size for reading

# Table Configuration
SOURCE_TABLE=your_source_table_name
TARGET_TABLE=your_target_table_name

# Logging
LOG_LEVEL=INFO
LOG_FILE=etl_transfer.log

# Connection timeouts (seconds)
CONNECTION_TIMEOUT=30
QUERY_TIMEOUT=300
```

## Usage

### Basic Transfer

Run a full transfer from source to target:

```bash
python main.py
```

### Drop and Recreate Target Table

If you want to start fresh and drop the existing target table:

```bash
python main.py --drop-target
```

### Incremental Transfer

For daily incremental transfers using a date column:

```bash
python main.py --incremental --date-column created_at
```

### Scheduled Transfer

Schedule daily transfers at a specific time:

```bash
# Run daily at 2:30 AM
python main.py --schedule "02:30"

# Run daily at 11:45 PM
python main.py --schedule "23:45"
```

### Command Line Options

```bash
python main.py [OPTIONS]

Options:
  --drop-target     Drop target table if it exists before transfer
  --incremental     Perform incremental transfer
  --date-column     Date column name for incremental transfer
  --schedule        Schedule time (e.g., "14:30" for daily at 2:30 PM)
  --run-now         Run transfer immediately (default behavior)
  -h, --help        Show help message
```

## Performance Optimization

### For 5+ Million Rows

The tool is optimized for large datasets with these default settings:

- **Batch Size**: 10,000 rows per batch
- **Parallel Workers**: 4 concurrent processes
- **Chunk Size**: 50,000 rows for pandas reading
- **Connection Pooling**: 5 connections with 10 overflow

### Adjusting Performance

Based on your system resources and network speed, you can adjust:

1. **Increase batch size** for faster processing (if you have more memory):
   ```env
   BATCH_SIZE=20000
   ```

2. **Increase parallel workers** for faster processing (if you have more CPU cores):
   ```env
   MAX_WORKERS=8
   ```

3. **Adjust chunk size** for pandas reading:
   ```env
   CHUNK_SIZE=100000
   ```

### Expected Performance

With default settings on a typical setup:
- **Transfer Rate**: 1,000-5,000 rows/second (depending on network and data size)
- **5 Million Rows**: Approximately 15-45 minutes
- **Memory Usage**: ~500MB-1GB (depending on row size)

## Monitoring and Logging

### Log Files

The tool creates detailed logs in `etl_transfer.log` with:
- Connection status
- Batch processing progress
- Error details
- Performance metrics
- Transfer completion summary

### Progress Tracking

Real-time progress bars show:
- Number of batches processed
- Rows transferred
- Success/failure counts
- Estimated time remaining

### Example Log Output

```
2024-01-15 14:30:00 - ETLTransfer - INFO - Starting ETL Transfer Process
2024-01-15 14:30:01 - ETLTransfer - INFO - Database connections validated successfully
2024-01-15 14:30:02 - ETLTransfer - INFO - Retrieved schema with 15 columns
2024-01-15 14:30:03 - ETLTransfer - INFO - Total rows in source table: 5,000,000
2024-01-15 14:30:03 - ETLTransfer - INFO - Will process 500 batches of 10,000 rows each
Processing batches: 100%|██████████| 500/500 [25:30<00:00, 3.06s/batch]
2024-01-15 14:55:33 - ETLTransfer - INFO - Transfer completed in 1530.45 seconds
2024-01-15 14:55:33 - ETLTransfer - INFO - Rows transferred: 5,000,000
2024-01-15 14:55:33 - ETLTransfer - INFO - Transfer rate: 3,267.32 rows/second
```

## Error Handling

The tool includes comprehensive error handling:

- **Connection Failures**: Automatic retry with exponential backoff
- **Batch Failures**: Individual batch failures don't stop the entire process
- **Memory Issues**: Configurable batch sizes to prevent memory overflow
- **Network Timeouts**: Configurable timeout settings
- **Schema Mismatches**: Automatic schema validation and creation

## Security Best Practices

1. **Use Environment Variables**: Never hardcode database credentials
2. **Network Security**: Ensure RDS security groups allow connections
3. **SSL Connections**: Consider enabling SSL for database connections
4. **IAM Authentication**: Use AWS IAM database authentication when possible
5. **VPC Access**: Run the tool from within the same VPC as your RDS instances

## Troubleshooting

### Common Issues

1. **Connection Timeout**:
   - Check network connectivity
   - Verify RDS security group settings
   - Increase `CONNECTION_TIMEOUT` in config

2. **Memory Issues**:
   - Reduce `BATCH_SIZE` and `CHUNK_SIZE`
   - Close other applications
   - Monitor system memory usage

3. **Slow Performance**:
   - Increase `MAX_WORKERS` (if CPU allows)
   - Check network bandwidth
   - Optimize RDS instance size

4. **Schema Errors**:
   - Verify table names in config
   - Check column data types
   - Ensure proper permissions

### Debug Mode

Enable debug logging for detailed troubleshooting:

```env
LOG_LEVEL=DEBUG
```

## Production Deployment

### As a Service

For production environments, consider running as a system service:

```bash
# Create systemd service file
sudo nano /etc/systemd/system/etl-transfer.service
```

```ini
[Unit]
Description=ETL Transfer Service
After=network.target

[Service]
Type=simple
User=etl-user
WorkingDirectory=/path/to/etl-tool
ExecStart=/usr/bin/python3 main.py --schedule "02:00"
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Docker Deployment

Create a Dockerfile for containerized deployment:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "main.py", "--schedule", "02:00"]
```

## Support

For issues or questions:
1. Check the log files for detailed error messages
2. Verify your configuration settings
3. Test database connectivity manually
4. Review the troubleshooting section above

## License

This tool is provided as-is for educational and production use. Please ensure compliance with your organization's security and data handling policies. 