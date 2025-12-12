import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionExtractor:
    def __init__(self, source_type='csv'):
        self.source_type = source_type
        self.logger = logging.getLogger(__name__)
    
    def extract_from_csv(self, file_path):
        """Extract data from CSV file"""
        try:
            self.logger.info(f"Extracting data from {file_path}")
            df = pd.read_csv(file_path, parse_dates=['transaction_date'])
            
            # Validate required columns
            required_columns = [
                'transaction_id', 'sender_phone', 'receiver_phone', 
                'transaction_type', 'amount', 'transaction_date'
            ]
            
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            self.logger.info(f"Successfully extracted {len(df)} records from CSV")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting from CSV: {str(e)}")
            raise
    
    def extract_from_database(self, connection_string, query):
        """Extract data from database"""
        try:
            engine = create_engine(connection_string)
            df = pd.read_sql(query, engine)
            self.logger.info(f"Successfully extracted {len(df)} records from database")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting from database: {str(e)}")
            raise
    
    def extract_recent_transactions(self, days=7):
        """Extract recent transactions (last N days)"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # For this example, we'll read from CSV with date filtering
            df = self.extract_from_csv('data/raw_mpesa_transactions.csv')
            
            # Filter for recent transactions
            filtered_df = df[df['transaction_date'] >= start_date]
            
            self.logger.info(f"Extracted {len(filtered_df)} recent transactions")
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Error extracting recent transactions: {str(e)}")
            raise

# Test the extractor
if __name__ == "__main__":
    extractor = TransactionExtractor()
    
    # Extract from CSV
    df = extractor.extract_from_csv('data/raw_mpesa_transactions.csv')
    print(f"Sample data shape: {df.shape}")
    print(df.head())