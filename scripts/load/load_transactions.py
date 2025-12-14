import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from scripts.database import DatabaseManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionLoader:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = logging.getLogger(__name__)
    
    def load_raw_transactions(self, df):
        """Load raw transactions to raw_transactions table"""
        try:
            # Ensure data types are correct
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            df['load_timestamp'] = pd.to_datetime(datetime.now())
            
            # Load to database
            self.db_manager.load_data_to_table(df, 'raw_transactions', if_exists='append')
            
            self.logger.info(f"Loaded {len(df)} raw transactions to database")
            
        except Exception as e:
            self.logger.error(f"Error loading raw transactions: {str(e)}")
            raise
    
    def load_transformed_transactions(self, df):
        """Load transformed transactions to transformed_transactions table"""
        try:
            # Ensure data types are correct
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            df['date_part_date'] = pd.to_datetime(df['date_part_date']).dt.date
            df['load_timestamp'] = pd.to_datetime(datetime.now())
            
            # Load to database
            self.db_manager.load_data_to_table(df, 'transformed_transactions', if_exists='append')
            
            self.logger.info(f"Loaded {len(df)} transformed transactions to database")
            
        except Exception as e:
            self.logger.error(f"Error loading transformed transactions: {str(e)}")
            raise
    
    def generate_daily_aggregates(self, date_str=None):
        """Generate daily transaction aggregates"""
        try:
            if date_str is None:
                date_str = datetime.now().strftime('%Y-%m-%d')
            
            # Aggregate query
            agg_query = f"""
            INSERT INTO daily_transaction_summary (
                summary_date, total_transactions, total_amount, total_fees,
                avg_transaction_amount, max_transaction_amount, unique_users,
                fraud_attempts, successful_transactions, failed_transactions
            )
            SELECT 
                date_part_date as summary_date,
                COUNT(*) as total_transactions,
                SUM(amount) as total_amount,
                SUM(fee) as total_fees,
                AVG(amount) as avg_transaction_amount,
                MAX(amount) as max_transaction_amount,
                COUNT(DISTINCT sender_phone) as unique_users,
                COUNT(CASE WHEN fraud_risk_score > 70 THEN 1 END) as fraud_attempts,
                COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_transactions,
                COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_transactions
            FROM transformed_transactions
            WHERE date_part_date = '{date_str}'
            GROUP BY date_part_date
            ON CONFLICT (summary_date) DO UPDATE SET
                total_transactions = EXCLUDED.total_transactions,
                total_amount = EXCLUDED.total_amount,
                total_fees = EXCLUDED.total_fees,
                avg_transaction_amount = EXCLUDED.avg_transaction_amount,
                max_transaction_amount = EXCLUDED.max_transaction_amount,
                unique_users = EXCLUDED.unique_users,
                fraud_attempts = EXCLUDED.fraud_attempts,
                successful_transactions = EXCLUDED.successful_transactions,
                failed_transactions = EXCLUDED.failed_transactions;
            """
            
            self.db_manager.execute_query(agg_query)
            
            self.logger.info(f"Generated daily aggregates for {date_str}")
            
        except Exception as e:
            self.logger.error(f"Error generating daily aggregates: {str(e)}")
            raise
    
    def create_fraud_alerts(self):
        """Create fraud alerts for high-risk transactions"""
        try:
            # Query for high-risk transactions
            fraud_query = """
            SELECT 
                transaction_id,
                transaction_date as alert_timestamp,
                fraud_risk_score as risk_score,
                fraud_category as alert_type
            FROM transformed_transactions
            WHERE fraud_risk_score > 70
              AND status = 'COMPLETED';
            """
            
            fraud_records = self.db_manager.execute_query(fraud_query)
            
            if fraud_records:
                # Create fraud alerts DataFrame
                fraud_df = pd.DataFrame(fraud_records, 
                                      columns=['transaction_id', 'alert_timestamp', 
                                             'risk_score', 'alert_type'])
                
                # Insert into fraud alerts table
                for _, row in fraud_df.iterrows():
                    insert_query = """
                    INSERT INTO fraud_alerts 
                        (transaction_id, alert_timestamp, risk_score, alert_type)
                    VALUES 
                        (%s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING;
                    """
                    
                    self.db_manager.execute_query(
                        insert_query, 
                        (row['transaction_id'], row['alert_timestamp'], 
                         row['risk_score'], row['alert_type'])
                    )
                
                self.logger.info(f"Created {len(fraud_df)} fraud alerts")
            else:
                self.logger.info("No high-risk transactions found")
                
        except Exception as e:
            self.logger.error(f"Error creating fraud alerts: {str(e)}")
            raise

# Test the loader
if __name__ == "__main__":
    # Create sample transformed data (using the same sample from transformer)
    sample_data = {
        'transaction_id': ['TXN_001', 'TXN_002', 'TXN_003'],
        'sender_phone': ['254712345678', '254712345679', '254712345680'],
        'receiver_phone': ['254712345679', '254712345680', '254712345681'],
        'transaction_type': ['P2P_TRANSFER', 'MERCHANT_PAYMENT', 'AIRTIME_TOPUP'],
        'amount': [1500.50, 2500.00, 300.00],
        'fee': [29.85, 49.75, 5.97],
        'transaction_date': [
            datetime(2024, 1, 1, 10, 30, 0),
            datetime(2024, 1, 1, 14, 45, 0),
            datetime(2024, 1, 1, 16, 20, 0)
        ],
        'date_part_date': [
            datetime(2024, 1, 1).date(),
            datetime(2024, 1, 1).date(),
            datetime(2024, 1, 1).date()
        ],
        'year': [2024, 2024, 2024],
        'month': [1, 1, 1],
        'day_of_week': [0, 0, 0],  # Monday
        'hour_of_day': [10, 14, 16],
        'location': ['Nairobi', 'Mombasa', 'Kisumu'],
        'currency': ['KES', 'KES', 'KES'],
        'status': ['COMPLETED', 'COMPLETED', 'COMPLETED'],
        'fraud_risk_score': [15, 65, 5],
        'fraud_category': ['Low Risk', 'Medium Risk', 'Low Risk'],
        'merchant_id': [None, 'MERCHANT_1001', None],
        'reference_number': ['REF001', 'REF002', 'REF003'],
        'channel': ['APP', 'USSD', 'WEB'],
        'category': ['Person-to-Person', 'Business Payments', 'Airtime & Data'],
        'sender_region': ['Central Kenya', 'Coastal Kenya', 'Western Kenya'],
        'receiver_region': ['Coastal Kenya', 'Rift Valley', 'Rift Valley'],
        'transaction_volume_category': ['Medium', 'Large', 'Small'],
        'is_suspicious_velocity': [0, 0, 0],
        'time_since_prev_transaction': [None, None, None]
    }
    
    df_sample = pd.DataFrame(sample_data)
    
    loader = TransactionLoader()
    
    # Load the sample data
    loader.load_transformed_transactions(df_sample)
    
    # Generate daily aggregates
    loader.generate_daily_aggregates('2024-01-01')
    
    print("Sample data loaded successfully!")