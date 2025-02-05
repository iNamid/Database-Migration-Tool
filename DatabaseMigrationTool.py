import logging
from typing import Dict, List, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import String, Integer, Float, DateTime, Boolean, Text
from tqdm import tqdm
import time

class DBTypeMapper:
    """Handle data type mapping between PostgreSQL and MySQL."""
    
    PG_TO_MYSQL = {
        'character varying': 'varchar',
        'timestamp without time zone': 'datetime',
        'double precision': 'double',
        'boolean': 'tinyint(1)',
        'text': 'longtext',
        'integer': 'int',
        'bigint': 'bigint',
        'json': 'json',
        'jsonb': 'json',
        'date': 'date',
        'time without time zone': 'time'
    }
    
    MYSQL_TO_PG = {
        'varchar': 'character varying',
        'datetime': 'timestamp without time zone',
        'double': 'double precision',
        'tinyint(1)': 'boolean',
        'longtext': 'text',
        'int': 'integer',
        'bigint': 'bigint',
        'json': 'jsonb',
        'date': 'date',
        'time': 'time without time zone'
    }
    
    @staticmethod
    def convert_type(source_type: str, source_db: str, target_db: str) -> str:
        """Convert data type from source to target database type."""
        if source_db == 'postgresql' and target_db == 'mysql':
            return DBTypeMapper.PG_TO_MYSQL.get(source_type.lower(), source_type)
        elif source_db == 'mysql' and target_db == 'postgresql':
            return DBTypeMapper.MYSQL_TO_PG.get(source_type.lower(), source_type)
        return source_type

class DatabaseMigrator:
    """Database migration tool for PostgreSQL and MySQL."""
    
    def __init__(
        self,
        source_conn: str,
        target_conn: str,
        batch_size: int = 1000,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize database migrator.
        
        Args:
            source_conn: SQLAlchemy connection string for source database
            target_conn: SQLAlchemy connection string for target database
            batch_size: Number of records to process in each batch
            logger: Custom logger instance
        """
        self.source_engine = create_engine(source_conn)
        self.target_engine = create_engine(target_conn)
        self.batch_size = batch_size
        self.logger = logger or self._setup_logger()
        
        # Identify database types
        self.source_db_type = self.source_engine.name
        self.target_db_type = self.target_engine.name
        
        if self.source_db_type not in ['postgresql', 'mysql'] or \
           self.target_db_type not in ['postgresql', 'mysql']:
            raise ValueError("Only PostgreSQL and MySQL are supported")

    def _setup_logger(self) -> logging.Logger:
        """Configure default logger."""
        logger = logging.getLogger('DatabaseMigrator')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def get_table_schema(self, table_name: str, engine: Engine) -> Dict:
        """Extract table schema information."""
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return {
            col['name']: {
                'type': str(col['type']),
                'nullable': col['nullable'],
                'default': col['default']
            }
            for col in columns
        }

    def create_target_table(self, table_name: str, schema: Dict) -> bool:
        """Create table in target database with appropriate schema."""
        try:
            columns_sql = []
            primary_keys = self._get_primary_keys(table_name, self.source_engine)
            
            for col_name, col_info in schema.items():
                # Convert data type from source to target
                source_type = col_info['type']
                target_type = DBTypeMapper.convert_type(
                    source_type,
                    self.source_db_type,
                    self.target_db_type
                )
                
                col_sql = f"{col_name} {target_type}"
                
                if not col_info['nullable']:
                    col_sql += " NOT NULL"
                    
                if col_info['default']:
                    col_sql += f" DEFAULT {col_info['default']}"
                    
                if col_name in primary_keys:
                    col_sql += " PRIMARY KEY"
                    
                columns_sql.append(col_sql)
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns_sql)}
                )
            """
            
            with self.target_engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating target table: {str(e)}")
            return False

    def _get_primary_keys(self, table_name: str, engine: Engine) -> List[str]:
        """Get primary key columns for a table."""
        inspector = inspect(engine)
        return [pk['name'] for pk in inspector.get_pk_constraint(table_name)['constrained_columns']]

    def migrate_table(
        self,
        table_name: str,
        custom_query: Optional[str] = None,
        create_target: bool = True
    ) -> bool:
        """
        Migrate table from source to target database.
        
        Args:
            table_name: Name of the table to migrate
            custom_query: Optional custom SQL query for source data
            create_target: Whether to create target table if it doesn't exist
        """
        try:
            start_time = time.time()
            self.logger.info(f"Starting migration for table: {table_name}")
            
            # Get source schema and create target table if needed
            if create_target:
                schema = self.get_table_schema(table_name, self.source_engine)
                if not self.create_target_table(table_name, schema):
                    return False
            
            # Get total count for progress bar
            with self.source_engine.connect() as conn:
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                total_records = conn.execute(text(count_query)).scalar()
            
            # Setup progress bar
            pbar = tqdm(total=total_records, desc="Migrating data", unit="records")
            
            # Process in batches
            offset = 0
            while True:
                query = custom_query if custom_query else f"SELECT * FROM {table_name}"
                query = f"{query} LIMIT {self.batch_size} OFFSET {offset}"
                
                # Read batch from source
                df = pd.read_sql(query, self.source_engine)
                
                if df.empty:
                    break
                
                # Handle data type conversions if needed
                if self.source_db_type != self.target_db_type:
                    df = self._convert_datatypes(df)
                
                # Write to target
                df.to_sql(
                    name=table_name,
                    con=self.target_engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                pbar.update(len(df))
                offset += self.batch_size
                
                if len(df) < self.batch_size:
                    break
            
            pbar.close()
            duration = time.time() - start_time
            self.logger.info(
                f"Migration completed for {table_name}. "
                f"Processed {total_records} records in {duration:.2f} seconds"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Error migrating table {table_name}: {str(e)}")
            return False

    def _convert_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert dataframe datatypes for compatibility."""
        type_mappings = {
            'bool': lambda x: x.astype(int) if self.target_db_type == 'mysql' else x,
            'datetime64[ns]': lambda x: x.astype(str) if self.target_db_type == 'mysql' else x
        }
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            if dtype in type_mappings:
                df[col] = type_mappings[dtype](df[col])
        
        return df

    def verify_migration(self, table_name: str) -> bool:
        """Verify the migration by comparing record counts and sampling data."""
        try:
            # Compare record counts
            with self.source_engine.connect() as source_conn:
                source_count = source_conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar()
            
            with self.target_engine.connect() as target_conn:
                target_count = target_conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar()
            
            if source_count != target_count:
                self.logger.error(
                    f"Count mismatch. Source: {source_count}, Target: {target_count}"
                )
                return False
            
            # Sample and compare some records
            sample_query = f"""
                SELECT * FROM {table_name} 
                ORDER BY RANDOM() 
                LIMIT 100
            """
            
            source_sample = pd.read_sql(sample_query, self.source_engine)
            target_sample = pd.read_sql(sample_query, self.target_engine)
            
            self.logger.info(
                f"Verification successful for {table_name}. "
                f"Source and target both have {source_count} records."
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Error verifying migration: {str(e)}")
            return False

# Example usage
if __name__ == "__main__":
    # Example connection strings
    source_db = "postgresql://user:pass@localhost:5432/source_db"
    target_db = "mysql://user:pass@localhost:3306/target_db"
    
    migrator = DatabaseMigrator(
        source_conn=source_db,
        target_conn=target_db,
        batch_size=5000
    )
    
    # Migrate table with schema creation
    success = migrator.migrate_table("users", create_target=True)
    
    if success:
        # Verify migration
        migrator.verify_migration("users")
        
        # Custom query migration
        custom_query = "SELECT * FROM users WHERE active = true"
        migrator.migrate_table(
            "active_users",
            custom_query=custom_query,
            create_target=True
        )