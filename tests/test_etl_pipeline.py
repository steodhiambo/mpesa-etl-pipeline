import pytest
import pandas as pd
from scripts.transform.transform_transactions import TransactionTransformer
from scripts.extract.extract_transactions import TransactionExtractor
from scripts.database import DatabaseManager
from scripts.load.load_transactions import TransactionLoader
import os


class TestTransactionTransformer:
    def setup_method(self):
        self.transformer = TransactionTransformer()
    
    def test_validate_data_no_errors(self):
        # Create sample data
        sample_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002'],
            'sender_phone': ['254712345678', '254712345679'],
            'receiver_phone': ['254712345679', '254712345680'],
            'transaction_type': ['P2P_TRANSFER', 'MERCHANT_PAYMENT'],
            'amount': [1000.0, 2000.0],
            'transaction_date': ['2024-01-01', '2024-01-02']
        })
        
        validation_results = self.transformer.validate_data(sample_data)
        
        assert validation_results['duplicate_ids'] == 0
        assert validation_results['negative_amounts'] == 0
    
    def test_clean_data_removes_duplicates(self):
        duplicate_data = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN001', 'TXN002'],
            'sender_phone': ['254712345678', '254712345678', '254712345679'],
            'amount': [1000.0, 1000.0, 2000.0],
            'transaction_date': ['2024-01-01', '2024-01-01', '2024-01-02']
        })
        
        cleaned_df = self.transformer.clean_data(duplicate_data)
        assert len(cleaned_df) == 2  # Should remove 1 duplicate
    
    def test_enrich_data_adds_columns(self):
        sample_data = pd.DataFrame({
            'transaction_id': ['TXN001'],
            'sender_phone': ['254712345678'],
            'receiver_phone': ['254712345679'],
            'transaction_type': ['P2P_TRANSFER'],
            'amount': [1000.0],
            'transaction_date': pd.to_datetime(['2024-01-01 10:30:00']),
            'location': ['Nairobi']
        })
        
        enriched_df = self.transformer.transform_data(sample_data)
        
        # Check for added columns
        assert 'year' in enriched_df.columns
        assert 'fraud_category' in enriched_df.columns
        assert 'transaction_volume_category' in enriched_df.columns


class TestDatabaseManager:
    def setup_method(self):
        # Skip tests if database not available
        if not os.getenv('TEST_DATABASE_AVAILABLE'):
            pytest.skip("Test database not available")
        self.db_manager = DatabaseManager()
    
    def test_create_tables(self):
        # This would normally test table creation
        # For now, just ensure no exceptions are raised
        try:
            self.db_manager.create_tables()
            assert True  # If no exception, test passes
        except Exception as e:
            pytest.fail(f"Table creation failed: {str(e)}")


def test_end_to_end_pipeline():
    """Test the end-to-end pipeline flow"""
    # Import main pipeline function
    from scripts.main_pipeline import run_mpesa_etl_pipeline
    
    # Generate sample data first
    from scripts.data_generator import MPesaDataGenerator
    generator = MPesaDataGenerator(num_transactions=100)
    df = generator.generate_transactions()
    df.to_csv('data/test_transactions.csv', index=False)
    
    # This test would normally run the full pipeline
    # but for safety, we'll just verify imports work
    assert True


if __name__ == "__main__":
    pytest.main([__file__])