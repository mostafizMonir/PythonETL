import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from typing import Optional, List, Dict, Any
from config import Config

class DatabaseManager:
    def __init__(self, connection_string: str, timeout: int = 30):
        """
        Initialize database manager with connection string
        
        Args:
            connection_string: PostgreSQL connection string
            timeout: Connection timeout in seconds
        """
        self.connection_string = connection_string
        self.timeout = timeout
        self.engine = None
        self.logger = logging.getLogger(__name__)
    
    def get_engine(self):
        """Get SQLAlchemy engine with connection pooling"""
        if self.engine is None:
            self.engine = create_engine(
                self.connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
                connect_args={
                    'connect_timeout': self.timeout,
                    'options': f'-c statement_timeout={Config.QUERY_TIMEOUT * 1000}'
                }
            )
        return self.engine
    
    def get_connection(self):
        """Get direct psycopg2 connection"""
        return psycopg2.connect(
            self.connection_string,
            connect_timeout=self.timeout
        )
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def create_schema_if_not_exists(self, schema_name: str):
        """Create schema if it doesn't exist"""
        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()
            self.logger.info(f"Schema '{schema_name}' created/verified successfully")
        except Exception as e:
            self.logger.error(f"Failed to create schema '{schema_name}': {e}")
            raise
    
    def get_table_schema(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """Get table schema information"""
        if schema_name:
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
            """
            params = (table_name, schema_name)
        else:
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
            """
            params = (table_name,)
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    columns = cursor.fetchall()
                    
                    schema = []
                    for col in columns:
                        schema.append({
                            'name': col[0],
                            'type': col[1],
                            'nullable': col[2] == 'YES',
                            'default': col[3]
                        })
                    return schema
        except Exception as e:
            self.logger.error(f"Failed to get schema for table {table_name}: {e}")
            return []
    
    def get_row_count(self, table_name: str, schema_name: str = None, where_clause: str = "") -> int:
        """Get total row count for a table"""
        if schema_name:
            query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
        else:
            query = f"SELECT COUNT(*) FROM {table_name}"
            
        if where_clause:
            query += f" WHERE {where_clause}"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                    return result[0]
        except Exception as e:
            self.logger.error(f"Failed to get row count for table {table_name}: {e}")
            return 0
    
    def create_table_if_not_exists(self, table_name: str, schema_name: str, schema: List[Dict[str, Any]], 
                                  drop_if_exists: bool = False):
        """Create table if it doesn't exist in specified schema"""
        # First create the schema if it doesn't exist
        self.create_schema_if_not_exists(schema_name)
        
        if drop_if_exists:
            drop_query = f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE"
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(drop_query)
                        conn.commit()
                self.logger.info(f"Dropped table {schema_name}.{table_name}")
            except Exception as e:
                self.logger.error(f"Failed to drop table {schema_name}.{table_name}: {e}")
        
        # Build CREATE TABLE statement
        columns = []
        for col in schema:
            col_def = f"{col['name']} {col['type']}"
            if not col['nullable']:
                col_def += " NOT NULL"
            if col['default']:
                col_def += f" DEFAULT {col['default']}"
            columns.append(col_def)
        
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {', '.join(columns)}
        )
        """
        
        try:            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_query)
                    conn.commit()
            self.logger.info(f"Created table {schema_name}.{table_name}")
        except Exception as e:
            self.logger.error(f"Failed to create table {schema_name}.{table_name}: {e}")
            raise
    
    def truncate_table(self, table_name: str, schema_name: str = None):
        """Truncate a table in the specified schema"""
        if schema_name:
            query = f"TRUNCATE TABLE {schema_name}.{table_name}"
        else:
            query = f"TRUNCATE TABLE {table_name}"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()
            self.logger.info(f"Truncated table {schema_name}.{table_name}" if schema_name else f"Truncated table {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to truncate table {schema_name}.{table_name}" if schema_name else f"Failed to truncate table {table_name}: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> Optional[List]:
        """Execute a query and return results"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    if query.strip().upper().startswith('SELECT'):
                        return cursor.fetchall()
                    else:
                        conn.commit()
                        return None
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def create_temp_table_from_source(self, source_table: str, source_schema: str, 
                                    temp_table_name: str, temp_schema: str, 
                                    offset: int, limit: int) -> bool:
        """
        Create a temporary table with a subset of data from source table
        
        Args:
            source_table: Name of the source table
            source_schema: Schema of the source table
            temp_table_name: Name for the temporary table
            temp_schema: Schema for the temporary table
            offset: Offset for data selection
            limit: Number of rows to select
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # First create the schema if it doesn't exist
            self.create_schema_if_not_exists(temp_schema)
            
            # Create temporary table with data from source
            create_query = f"""
            CREATE TABLE {temp_schema}.{temp_table_name} AS
            SELECT * FROM {source_schema}.{source_table}
            ORDER BY 1  -- Order by first column for consistent pagination
            LIMIT %s OFFSET %s
            """
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_query, (limit, offset))
                    conn.commit()
            
            # Get the actual number of rows created
            actual_rows = self.get_row_count(temp_table_name, temp_schema)
            self.logger.info(f"Created temporary table {temp_schema}.{temp_table_name} with {actual_rows:,} rows")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create temporary table {temp_schema}.{temp_table_name}: {e}")
            return False
    
    def drop_table(self, table_name: str, schema_name: str) -> bool:
        """
        Drop a table from the specified schema
        
        Args:
            table_name: Name of the table to drop
            schema_name: Schema containing the table
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            drop_query = f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE"
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(drop_query)
                    conn.commit()
            
            self.logger.info(f"Dropped table {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to drop table {schema_name}.{table_name}: {e}")
            return False
    
    def get_table_names_in_schema(self, schema_name: str, pattern: str = None) -> List[str]:
        """
        Get list of table names in a schema
        
        Args:
            schema_name: Name of the schema
            pattern: Optional pattern to filter table names (e.g., 'event_plan_member_%')
            
        Returns:
            List[str]: List of table names
        """
        try:
            if pattern:
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name LIKE %s
                ORDER BY table_name
                """
                params = (schema_name, pattern)
            else:
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name
                """
                params = (schema_name,)
            
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    tables = cursor.fetchall()
                    return [table[0] for table in tables]
                    
        except Exception as e:
            self.logger.error(f"Failed to get table names in schema {schema_name}: {e}")
            return []
    
    def extract_from_temp_table(self, temp_table_name: str, temp_schema: str, 
                              offset: int, limit: int) -> pd.DataFrame:
        """
        Extract data from a temporary table using offset and limit
        
        Args:
            temp_table_name: Name of the temporary table
            temp_schema: Schema of the temporary table
            offset: Offset for data selection
            limit: Number of rows to select
            
        Returns:
            pd.DataFrame: DataFrame containing the extracted data
        """
        query = f"""
        SELECT * FROM {temp_schema}.{temp_table_name}
        ORDER BY 1  -- Order by first column for consistent pagination
        LIMIT %s OFFSET %s
        """
        
        try:
            engine = self.get_engine()
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
            
            return df
                
        except Exception as e:
            self.logger.error(f"Failed to extract from temp table {temp_schema}.{temp_table_name} (offset={offset}, limit={limit}): {e}")
            raise 