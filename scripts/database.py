import pandas as pd
from sqlalchemy import create_engine, text, pool
from sqlalchemy.exc import SQLAlchemyError
import logging
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.connection_string = self._build_connection_string()
        self.engine = self.create_engine()
    
    def _build_connection_string(self):
        """Build secure database connection string"""
        db_type = os.getenv('DB_TYPE', 'sqlite')
        
        if db_type == 'sqlite':
            db_name = os.getenv('DB_NAME', 'mpesa_analytics.db')
            return f"sqlite:///data/{db_name}"
        
        # PostgreSQL connection with proper URL encoding
        required_vars = ['POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        user = quote_plus(os.getenv('POSTGRES_USER'))
        password = quote_plus(os.getenv('POSTGRES_PASSWORD'))
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB')
        
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

    def create_engine(self):
        """Create database engine with connection pooling and security settings"""
        try:
            engine_kwargs = {
                'echo': os.getenv('LOG_LEVEL') == 'DEBUG',
                'pool_pre_ping': True,
                'pool_recycle': 3600,
                'connect_args': {}
            }
            
            # Add SSL settings for PostgreSQL in production
            if 'postgresql' in self.connection_string and os.getenv('ENVIRONMENT') == 'production':
                engine_kwargs['connect_args']['sslmode'] = 'require'
            
            engine = create_engine(self.connection_string, **engine_kwargs)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            
            self.logger.info("Database connection established successfully")
            return engine
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during database connection: {str(e)}")
            raise

    def create_tables(self):
        """Create necessary tables for the ETL pipeline"""
        with self.engine.connect() as conn:
            # Raw transactions table
            raw_table_query = """
            CREATE TABLE IF NOT EXISTS raw_transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                sender_phone VARCHAR(20),
                receiver_phone VARCHAR(20),
                transaction_type VARCHAR(50),
                amount DECIMAL(15,2),
                fee DECIMAL(10,2),
                transaction_date TIMESTAMP,
                location VARCHAR(100),
                currency VARCHAR(3),
                status VARCHAR(20),
                fraud_risk_score INTEGER,
                merchant_id VARCHAR(50),
                reference_number VARCHAR(100),
                channel VARCHAR(20),
                category VARCHAR(50),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """

            # Transformed transactions table
            transformed_table_query = """
            CREATE TABLE IF NOT EXISTS transformed_transactions (
                transaction_key BIGSERIAL PRIMARY KEY,
                transaction_id VARCHAR(50),
                sender_phone VARCHAR(20),
                receiver_phone VARCHAR(20),
                transaction_type VARCHAR(50),
                amount DECIMAL(15,2),
                fee DECIMAL(10,2),
                transaction_date TIMESTAMP,
                date_part_date DATE,
                year INTEGER,
                month INTEGER,
                day_of_week INTEGER,
                hour_of_day INTEGER,
                location VARCHAR(100),
                currency VARCHAR(3),
                status VARCHAR(20),
                fraud_risk_score INTEGER,
                fraud_category VARCHAR(20),
                merchant_id VARCHAR(50),
                reference_number VARCHAR(100),
                channel VARCHAR(20),
                category VARCHAR(50),
                sender_region VARCHAR(100),
                receiver_region VARCHAR(100),
                transaction_volume_category VARCHAR(20),
                is_suspicious_velocity INTEGER,
                time_since_prev_transaction REAL,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """

            # Fraud alerts table
            fraud_alerts_query = """
            CREATE TABLE IF NOT EXISTS fraud_alerts (
                alert_id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50),
                alert_timestamp TIMESTAMP,
                risk_score INTEGER,
                alert_type VARCHAR(50),
                status VARCHAR(20) DEFAULT 'OPEN',
                analyst_assigned VARCHAR(50),
                resolution_notes TEXT,
                resolved_timestamp TIMESTAMP
            );
            """

            # Daily aggregates table
            daily_aggregates_query = """
            CREATE TABLE IF NOT EXISTS daily_transaction_summary (
                summary_date DATE PRIMARY KEY,
                total_transactions INTEGER,
                total_amount DECIMAL(20,2),
                total_fees DECIMAL(15,2),
                avg_transaction_amount DECIMAL(15,2),
                max_transaction_amount DECIMAL(15,2),
                unique_users INTEGER,
                fraud_attempts INTEGER,
                successful_transactions INTEGER,
                failed_transactions INTEGER
            );
            """

            # Execute all queries
            for query in [raw_table_query, transformed_table_query, fraud_alerts_query, daily_aggregates_query]:
                conn.execute(text(query))
            conn.commit()

            self.logger.info("Tables created successfully")

    def load_data_to_table(self, df, table_name, if_exists='append'):
        """Load DataFrame to database table with validation"""
        if df.empty:
            self.logger.warning(f"No data to load to {table_name}")
            return
        
        try:
            # Validate table name to prevent SQL injection
            if not table_name.replace('_', '').isalnum():
                raise ValueError(f"Invalid table name: {table_name}")
            
            df.to_sql(
                table_name, 
                self.engine, 
                if_exists=if_exists, 
                index=False, 
                method='multi',
                chunksize=1000
            )
            self.logger.info(f"Successfully loaded {len(df)} records to {table_name}")
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error loading data to {table_name}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error loading data to {table_name}: {str(e)}")
            raise

    def execute_query(self, query, params=None):
        """Execute a custom query with proper error handling"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                
                if query.strip().upper().startswith('SELECT'):
                    return result.fetchall()
                else:
                    conn.commit()
                    return result.rowcount
                    
        except SQLAlchemyError as e:
            self.logger.error(f"SQL execution failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise

# Test the database connection
if __name__ == "__main__":
    db_manager = DatabaseManager()
    db_manager.create_tables()
    print("Database setup completed successfully!")