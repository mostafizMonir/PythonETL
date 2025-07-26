import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Source database (AWS RDS - where data is extracted from)
    SOURCE_HOST = os.getenv('SOURCE_HOST', 'your-source-rds-endpoint.region.rds.amazonaws.com')
    SOURCE_PORT = os.getenv('SOURCE_PORT', '5432')
    SOURCE_DATABASE = os.getenv('SOURCE_DATABASE', 'your_source_db')
    SOURCE_USERNAME = os.getenv('SOURCE_USERNAME', 'your_username')
    SOURCE_PASSWORD = os.getenv('SOURCE_PASSWORD', 'your_password')
    
    # Target database (Warehouse RDS - where data is loaded to)
    TARGET_HOST = os.getenv('TARGET_HOST', 'your-warehouse-rds-endpoint.region.rds.amazonaws.com')
    TARGET_PORT = os.getenv('TARGET_PORT', '5432')
    TARGET_DATABASE = os.getenv('TARGET_DATABASE', 'your_warehouse_db')
    TARGET_USERNAME = os.getenv('TARGET_USERNAME', 'your_username')
    TARGET_PASSWORD = os.getenv('TARGET_PASSWORD', 'your_password')
    
    # ETL Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10000'))  # Number of rows to process in each batch
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))    # Number of parallel workers
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '50000'))  # Pandas chunk size for reading
    
    # Table Configuration
    SOURCE_TABLE = os.getenv('SOURCE_TABLE', 'your_source_table')
    TARGET_TABLE = os.getenv('TARGET_TABLE', 'your_target_table')
    TARGET_SCHEMA = os.getenv('TARGET_SCHEMA', 'ETL')  # Target schema name
    SOURCE_SCHEMA = os.getenv('SOURCE_SCHEMA', 'public')  # Source schema name
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'etl_transfer.log')
    
    # Connection timeout settings
    CONNECTION_TIMEOUT = int(os.getenv('CONNECTION_TIMEOUT', '30'))
    QUERY_TIMEOUT = int(os.getenv('QUERY_TIMEOUT', '300'))
    
    @classmethod
    def get_source_connection_string(cls):
        """Get source database connection string"""
        return f"postgresql://{cls.SOURCE_USERNAME}:{cls.SOURCE_PASSWORD}@{cls.SOURCE_HOST}:{cls.SOURCE_PORT}/{cls.SOURCE_DATABASE}"
    
    @classmethod
    def get_target_connection_string(cls):
        """Get target database connection string"""
        return f"postgresql://{cls.TARGET_USERNAME}:{cls.TARGET_PASSWORD}@{cls.TARGET_HOST}:{cls.TARGET_PORT}/{cls.TARGET_DATABASE}" 