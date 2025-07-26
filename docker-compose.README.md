# Docker Compose Setup for ETL Transfer Tool

This Docker Compose setup provides a containerized environment for running the ETL transfer tool that connects to external PostgreSQL databases (like AWS RDS).

## Quick Start

### 1. Development/Testing Environment

```bash
# Start the ETL application
docker-compose up -d

# View logs
docker-compose logs -f etl-app

# Stop the application
docker-compose down
```

### 2. Production Environment (with AWS RDS)

```bash
# Start with production configuration (connects to AWS RDS)
docker-compose -f docker-compose.prod.yml up -d

# View logs
docker-compose -f docker-compose.prod.yml logs -f etl-app
```

## Docker Compose Files

### `docker-compose.yml` (Default)
- **Purpose**: Development and testing with external databases
- **Features**: 
  - ETL application only
  - Environment variable configuration
  - Development-friendly settings

### `docker-compose.override.yml` (Development)
- **Purpose**: Override settings for development
- **Features**:
  - Immediate execution (no scheduling)
  - Debug logging
  - Smaller batch sizes for testing
  - Source code mounting for live development

### `docker-compose.prod.yml` (Production)
- **Purpose**: Production deployment with AWS RDS
- **Features**:
  - Optimized for large datasets
  - Higher batch sizes and worker counts
  - Longer timeouts
  - Production scheduling

## Services Overview

### ETL Application (`etl-app`)
- **Image**: Built from local Dockerfile
- **Purpose**: Main ETL transfer application
- **Ports**: None (internal service)
- **Volumes**: 
  - `./logs:/app/logs` - Log files
  - `./.env:/app/.env:ro` - Environment configuration

## Usage Scenarios

### Scenario 1: Development with External Databases

```bash
# 1. Create .env file with your database credentials
cp env_example.txt .env
# Edit .env with your actual database endpoints

# 2. Start the application
docker-compose up -d

# 3. Run ETL transfer
docker-compose exec etl-app python main.py --run-now

# 4. Check logs
docker-compose logs -f etl-app
```

### Scenario 2: Development with Live Code Changes

```bash
# 1. Start with development override
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d

# 2. Make code changes (they're automatically reflected)
# 3. Restart the ETL app
docker-compose restart etl-app
```

### Scenario 3: Production with AWS RDS

```bash
# 1. Create .env file with AWS RDS credentials
cp env_example.txt .env
# Edit .env with your actual AWS RDS endpoints

# 2. Start production stack
docker-compose -f docker-compose.prod.yml up -d

# 3. Monitor the transfer
docker-compose -f docker-compose.prod.yml logs -f etl-app
```

### Scenario 4: One-time Transfer (No Scheduling)

```bash
# Override the command for immediate execution
docker-compose run --rm etl-app python main.py --run-now
```

## Environment Configuration

### Development Environment Variables

Create a `.env` file for development:

```env
# Database Configuration (External databases)
SOURCE_HOST=your-source-db-host.com
TARGET_HOST=your-target-db-host.com
SOURCE_DATABASE=your_source_db
TARGET_DATABASE=your_target_db
SOURCE_USERNAME=your_username
TARGET_USERNAME=your_username
SOURCE_PASSWORD=your_password
TARGET_PASSWORD=your_password

# ETL Configuration
BATCH_SIZE=1000
MAX_WORKERS=2
SOURCE_TABLE=your_source_table
TARGET_TABLE=your_target_table

# Logging
LOG_LEVEL=DEBUG
```

### Production Environment Variables

For production with AWS RDS:

```env
# AWS RDS Source
SOURCE_HOST=your-source-rds.region.rds.amazonaws.com
SOURCE_DATABASE=your_source_db
SOURCE_USERNAME=your_username
SOURCE_PASSWORD=your_password

# AWS RDS Target (Warehouse)
TARGET_HOST=your-warehouse-rds.region.rds.amazonaws.com
TARGET_DATABASE=your_warehouse_db
TARGET_USERNAME=your_username
TARGET_PASSWORD=your_password

# Production ETL Settings
BATCH_SIZE=20000
MAX_WORKERS=8
SOURCE_TABLE=your_source_table
TARGET_TABLE=your_target_table
```

## Monitoring and Logs

### View Application Logs

```bash
# All services
docker-compose logs

# ETL application only
docker-compose logs etl-app

# Follow logs in real-time
docker-compose logs -f etl-app

# Last 100 lines
docker-compose logs --tail=100 etl-app
```

### Health Checks

```bash
# Check service health
docker-compose ps

# Check specific service
docker-compose exec etl-app python -c "from database_manager import DatabaseManager; from config import Config; db = DatabaseManager(Config.get_source_connection_string()); print('Connection OK' if db.test_connection() else 'Connection Failed')"
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```bash
   # Check if the application is running
   docker-compose ps
   
   # Check application logs
   docker-compose logs etl-app
   
   # Verify your .env file has correct credentials
   cat .env
   ```

2. **Permission Issues**
   ```bash
   # Fix log directory permissions
   mkdir -p logs
   chmod 755 logs
   ```

3. **Memory Issues**
   ```bash
   # Reduce batch size in .env
   BATCH_SIZE=1000
   MAX_WORKERS=2
   ```

4. **Network Issues**
   ```bash
   # Check if you can reach your databases
   docker-compose exec etl-app ping your-source-db-host.com
   ```

### Reset Everything

```bash
# Stop and remove everything
docker-compose down

# Remove all images
docker-compose down --rmi all

# Start fresh
docker-compose up -d
```

## Performance Tuning

### For Large Datasets (5M+ rows)

```env
# Production settings
BATCH_SIZE=20000
MAX_WORKERS=8
CHUNK_SIZE=100000
CONNECTION_TIMEOUT=60
QUERY_TIMEOUT=600
```

### For Development/Testing

```env
# Development settings
BATCH_SIZE=1000
MAX_WORKERS=2
CHUNK_SIZE=5000
LOG_LEVEL=DEBUG
```

## Security Considerations

1. **Never commit `.env` files** with real credentials
2. **Use AWS IAM authentication** when possible
3. **Run in private subnets** for production
4. **Use SSL connections** for database connections
5. **Regular security updates** for base images

## Production Deployment

### AWS ECS/Fargate

```bash
# Build and push to ECR
docker build -t your-ecr-repo/etl-transfer .
docker push your-ecr-repo/etl-transfer

# Use docker-compose.prod.yml with ECS
```

### Kubernetes

```bash
# Convert to Kubernetes manifests
kompose convert -f docker-compose.prod.yml
```

### Docker Swarm

```bash
# Deploy to swarm
docker stack deploy -c docker-compose.prod.yml etl-stack
```

## Database Connection Testing

### Test Source Database Connection

```bash
docker-compose exec etl-app python -c "
from database_manager import DatabaseManager
from config import Config
db = DatabaseManager(Config.get_source_connection_string())
print('Source DB Connection:', 'OK' if db.test_connection() else 'FAILED')
"
```

### Test Target Database Connection

```bash
docker-compose exec etl-app python -c "
from database_manager import DatabaseManager
from config import Config
db = DatabaseManager(Config.get_target_connection_string())
print('Target DB Connection:', 'OK' if db.test_connection() else 'FAILED')
"
```

### Get Table Information

```bash
docker-compose exec etl-app python -c "
from database_manager import DatabaseManager
from config import Config
db = DatabaseManager(Config.get_source_connection_string())
count = db.get_row_count(Config.SOURCE_TABLE)
print(f'Source table {Config.SOURCE_TABLE} has {count:,} rows')
" 