import pandas as pd
import numpy as np
import logging
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def validate_data(self, df):
        """Validate the extracted data"""
        validation_results = {}
        
        # Check for null values
        null_counts = df.isnull().sum()
        validation_results['null_values'] = null_counts[null_counts > 0].to_dict()
        
        # Check data types
        validation_results['dtypes'] = df.dtypes.to_dict()
        
        # Check for duplicate transaction IDs
        dup_count = df.duplicated(subset=['transaction_id']).sum()
        validation_results['duplicate_ids'] = dup_count
        
        # Check for negative amounts
        neg_amounts = (df['amount'] < 0).sum()
        validation_results['negative_amounts'] = neg_amounts
        
        return validation_results
    
    def clean_data(self, df):
        """Clean the transaction data"""
        self.logger.info("Starting data cleaning...")
        
        initial_rows = len(df)
        
        # Remove duplicates based on transaction_id
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        # Handle missing values
        # Fill numeric columns with median
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        for col in numeric_cols:
            if col in ['fraud_risk_score']:  # Special handling for fraud risk score
                df[col] = df[col].fillna(0)
            else:
                df[col] = df[col].fillna(df[col].median())
        
        # Fill categorical columns with mode
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        categorical_cols = [col for col in categorical_cols if col != 'transaction_id']
        
        for col in categorical_cols:
            if df[col].isnull().any():
                mode_val = df[col].mode()
                if len(mode_val) > 0:
                    df[col] = df[col].fillna(mode_val[0])
        
        # Remove rows with negative amounts (invalid transactions)
        df = df[df['amount'] >= 0]
        
        # Ensure transaction_date is datetime
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        # Cap fraud risk score between 0 and 100
        df['fraud_risk_score'] = df['fraud_risk_score'].clip(0, 100)
        
        final_rows = len(df)
        self.logger.info(f"Cleaned data from {initial_rows} to {final_rows} rows")
        
        return df
    
    def enrich_data(self, df):
        """Add derived fields and enrich the data"""
        self.logger.info("Starting data enrichment...")
        
        # Extract temporal features
        df['date_part_date'] = df['transaction_date'].dt.date
        df['year'] = df['transaction_date'].dt.year
        df['month'] = df['transaction_date'].dt.month
        df['day_of_week'] = df['transaction_date'].dt.dayofweek
        df['hour_of_day'] = df['transaction_date'].dt.hour
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Categorize transaction amounts
        df['transaction_volume_category'] = pd.cut(
            df['amount'],
            bins=[0, 100, 500, 2000, 10000, float('inf')],
            labels=['Very Small', 'Small', 'Medium', 'Large', 'Very Large']
        )
        
        # Categorize fraud risk levels
        df['fraud_category'] = pd.cut(
            df['fraud_risk_score'],
            bins=[0, 30, 70, 100],
            labels=['Low Risk', 'Medium Risk', 'High Risk']
        )
        
        # Add geographical information (simulated)
        kenyan_regions = {
            'Nairobi': 'Central Kenya',
            'Mombasa': 'Coastal Kenya',
            'Kisumu': 'Western Kenya',
            'Nakuru': 'Rift Valley',
            'Eldoret': 'Rift Valley',
            'Kisii': 'Western Kenya',
            'Kitale': 'Rift Valley',
            'Garissa': 'North Eastern',
            'Thika': 'Central Kenya',
            'Malindi': 'Coastal Kenya'
        }
        
        df['sender_region'] = df['location'].map(kenyan_regions).fillna('Other Region')
        df['receiver_region'] = df['location'].map(kenyan_regions).fillna('Other Region')
        
        # Calculate transaction velocity (simplified)
        df = df.sort_values(['sender_phone', 'transaction_date'])
        df['prev_transaction_time'] = df.groupby('sender_phone')['transaction_date'].shift(1)
        df['time_since_prev_transaction'] = (
            df['transaction_date'] - df['prev_transaction_time']
        ).dt.total_seconds() / 60  # in minutes
        
        # Flag suspicious patterns
        df['is_suspicious_velocity'] = (df['time_since_prev_transaction'] < 5).astype(int)
        
        self.logger.info("Data enrichment completed")
        
        return df
    
    def transform_data(self, df):
        """Complete transformation pipeline"""
        self.logger.info("Starting data transformation pipeline...")
        
        # Validate data
        validation_results = self.validate_data(df)
        self.logger.info(f"Validation results: {validation_results}")
        
        # Clean data
        df_cleaned = self.clean_data(df)
        
        # Enrich data
        df_enriched = self.enrich_data(df_cleaned)
        
        # Reorder columns for consistency
        expected_columns = [
            'transaction_id', 'sender_phone', 'receiver_phone', 'transaction_type',
            'amount', 'fee', 'transaction_date', 'date_part_date', 'year', 'month',
            'day_of_week', 'hour_of_day', 'location', 'currency', 'status',
            'fraud_risk_score', 'fraud_category', 'merchant_id', 'reference_number',
            'channel', 'category', 'sender_region', 'receiver_region',
            'transaction_volume_category', 'is_suspicious_velocity',
            'time_since_prev_transaction'
        ]
        
        # Ensure all expected columns exist
        for col in expected_columns:
            if col not in df_enriched.columns:
                if col in ['fee', 'fraud_risk_score']:
                    df_enriched[col] = 0  # Default values for numeric columns
                else:
                    df_enriched[col] = ''  # Default values for string columns
        
        df_final = df_enriched[expected_columns]
        
        self.logger.info(f"Transformation completed. Final shape: {df_final.shape}")
        
        return df_final

# Test the transformer
if __name__ == "__main__":
    # Load sample data to test transformation
    sample_data = {
        'transaction_id': ['TXN_001', 'TXN_002', 'TXN_003'],
        'sender_phone': ['254712345678', '254712345679', '254712345680'],
        'receiver_phone': ['254712345679', '254712345680', '254712345681'],
        'transaction_type': ['P2P_TRANSFER', 'MERCHANT_PAYMENT', 'AIRTIME_TOPUP'],
        'amount': [1500.50, 2500.00, 300.00],
        'fee': [29.85, 49.75, 5.97],
        'transaction_date': [
            '2024-01-01 10:30:00',
            '2024-01-01 14:45:00',
            '2024-01-01 16:20:00'
        ],
        'location': ['Nairobi', 'Mombasa', 'Kisumu'],
        'currency': ['KES', 'KES', 'KES'],
        'status': ['COMPLETED', 'COMPLETED', 'COMPLETED'],
        'fraud_risk_score': [15, 65, 5],
        'merchant_id': [None, 'MERCHANT_1001', None],
        'reference_number': ['REF001', 'REF002', 'REF003'],
        'channel': ['APP', 'USSD', 'WEB'],
        'category': ['Person-to-Person', 'Business Payments', 'Airtime & Data']
    }
    
    df_sample = pd.DataFrame(sample_data)
    df_sample['transaction_date'] = pd.to_datetime(df_sample['transaction_date'])
    
    transformer = TransactionTransformer()
    df_transformed = transformer.transform_data(df_sample)
    
    print("Sample transformed data:")
    print(df_transformed.head())
    print(f"\nTransformed data shape: {df_transformed.shape}")
    print(f"\nColumn info:\n{df_transformed.dtypes}")