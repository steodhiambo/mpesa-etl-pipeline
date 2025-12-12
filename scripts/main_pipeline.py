import pandas as pd
import logging
from datetime import datetime, timedelta
from scripts.extract.extract_transactions import TransactionExtractor
from scripts.transform.transform_transactions import TransactionTransformer
from scripts.load.load_transactions import TransactionLoader
from scripts.database import DatabaseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_mpesa_etl_pipeline():
    """Main M-Pesa ETL pipeline function"""
    logger.info("="*60)
    logger.info("M-Pesa Transaction Analytics Pipeline - START")
    logger.info(f"Pipeline run started at: {datetime.now()}")
    logger.info("="*60)
    
    try:
        # Initialize components
        extractor = TransactionExtractor()
        transformer = TransactionTransformer()
        loader = TransactionLoader()
        
        # Phase 1: Extract
        logger.info("[1/4] Starting Data Extraction Phase...")
        raw_df = extractor.extract_recent_transactions(days=7)  # Last 7 days
        logger.info(f"Extracted {len(raw_df)} records")
        
        # Load raw data to database (for audit trail)
        logger.info("Loading raw data to database...")
        loader.load_raw_transactions(raw_df)
        
        # Phase 2: Transform
        logger.info("[2/4] Starting Data Transformation Phase...")
        transformed_df = transformer.transform_data(raw_df)
        logger.info(f"Transformed data shape: {transformed_df.shape}")
        
        # Phase 3: Load
        logger.info("[3/4] Starting Data Loading Phase...")
        loader.load_transformed_transactions(transformed_df)
        
        # Phase 4: Generate Aggregates and Alerts
        logger.info("[4/4] Generating Aggregates and Alerts...")
        
        # Get unique dates to generate aggregates for
        unique_dates = transformed_df['date_part_date'].unique()
        for date in unique_dates:
            if isinstance(date, str):
                date_obj = datetime.strptime(date, '%Y-%m-%d').date()
            else:
                date_obj = date
            loader.generate_daily_aggregates(date_obj.strftime('%Y-%m-%d'))
        
        # Create fraud alerts
        loader.create_fraud_alerts()
        
        logger.info("All phases completed successfully!")
        
        # Log summary statistics
        logger.info(f"Pipeline Summary:")
        logger.info(f"  - Total transactions processed: {len(transformed_df)}")
        logger.info(f"  - Date range: {transformed_df['transaction_date'].min()} to {transformed_df['transaction_date'].max()}")
        logger.info(f"  - Transaction types: {transformed_df['transaction_type'].nunique()}")
        logger.info(f"  - Total amount processed: KES {transformed_df['amount'].sum():,.2f}")
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise
    
    finally:
        logger.info("="*60)
        logger.info("M-Pesa Transaction Analytics Pipeline - END")
        logger.info(f"Pipeline run completed at: {datetime.now()}")
        logger.info("="*60)

def run_specific_date_pipeline(target_date=None):
    """Run pipeline for a specific date"""
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Running date-specific pipeline for: {target_date}")
    
    try:
        # Extract just for the specific date
        extractor = TransactionExtractor()
        raw_df = extractor.extract_from_csv('data/raw_mpesa_transactions.csv')
        
        # Filter for target date
        raw_df['transaction_date'] = pd.to_datetime(raw_df['transaction_date'])
        target_date_dt = pd.to_datetime(target_date)
        daily_df = raw_df[raw_df['transaction_date'].dt.date == target_date_dt.date()]
        
        if len(daily_df) == 0:
            logger.info(f"No transactions found for {target_date}, skipping...")
            return
        
        # Transform and load
        transformer = TransactionTransformer()
        loader = TransactionLoader()
        
        transformed_df = transformer.transform_data(daily_df)
        loader.load_transformed_transactions(transformed_df)
        loader.generate_daily_aggregates(target_date)
        
        logger.info(f"Date-specific pipeline completed for {target_date}")
        
    except Exception as e:
        logger.error(f"Date-specific pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Ensure database tables exist
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    # Run the main pipeline
    run_mpesa_etl_pipeline()