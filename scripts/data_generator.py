import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import random
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MPesaDataGenerator:
    def __init__(self, num_transactions=10000):
        self.num_transactions = num_transactions
        self.fake = Faker('en_KE')  # Kenyan locale
        self.transaction_types = [
            'P2P_TRANSFER', 'MERCHANT_PAYMENT', 'BILL_PAYMENT', 
            'AIRTIME_TOPUP', 'WITHDRAWAL', 'DEPOSIT'
        ]
        
        # Kenyan cities and regions
        self.locations = [
            'Nairobi', 'Mombasa', 'Kisumu', 'Nakuru', 'Eldoret',
            'Kisii', 'Kitale', 'Garissa', 'Thika', 'Malindi'
        ]
        
        # Generate user base
        self.users = [f"{self.generate_mpesa_number()}" for _ in range(2000)]
    
    def generate_mpesa_number(self):
        """Generate realistic M-Pesa number"""
        return f"254{random.randint(700000000, 799999999)}"
    
    def generate_transaction_date(self):
        """Generate transaction dates within last 30 days"""
        start_date = datetime.now() - timedelta(days=30)
        random_date = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        return random_date
    
    def generate_amount(self, transaction_type):
        """Generate realistic transaction amounts based on type"""
        if transaction_type == 'AIRTIME_TOPUP':
            return round(random.uniform(50, 1000), 2)
        elif transaction_type == 'P2P_TRANSFER':
            return round(random.uniform(100, 5000), 2)
        elif transaction_type == 'MERCHANT_PAYMENT':
            return round(random.uniform(200, 10000), 2)
        elif transaction_type == 'BILL_PAYMENT':
            return round(random.uniform(1000, 50000), 2)
        elif transaction_type == 'WITHDRAWAL':
            return round(random.uniform(500, 20000), 2)
        else:  # DEPOSIT
            return round(random.uniform(100, 5000), 2)
    
    def generate_fraud_risk_score(self, amount, transaction_type):
        """Generate fraud risk score based on transaction characteristics"""
        risk_score = 0
        
        # Higher risk for very large amounts
        if amount > 50000:
            risk_score += 30
        elif amount > 10000:
            risk_score += 20
        elif amount > 5000:
            risk_score += 10
            
        # Certain transaction types are higher risk
        if transaction_type in ['P2P_TRANSFER']:
            risk_score += 5
            
        # Random fraud
        if random.random() < 0.02:  # 2% of transactions are flagged as suspicious
            risk_score += 40
            
        return min(100, risk_score + random.randint(0, 10))
    
    def generate_transactions(self):
        """Generate synthetic M-Pesa transaction data"""
        logger.info(f"Generating {self.num_transactions} synthetic transactions...")
        
        transactions = []
        for i in range(self.num_transactions):
            # Select random users
            sender = random.choice(self.users)
            # Ensure receiver is different from sender
            receivers = [u for u in self.users if u != sender]
            receiver = random.choice(receivers)
            
            # Generate transaction properties
            trans_type = random.choice(self.transaction_types)
            date = self.generate_transaction_date()
            amount = self.generate_amount(trans_type)
            location = random.choice(self.locations)
            fee = round(amount * 0.0199, 2)  # Typical M-Pesa fee rate
            
            # Calculate fraud risk
            fraud_risk = self.generate_fraud_risk_score(amount, trans_type)
            
            # Create transaction record
            transaction = {
                'transaction_id': f"TXN_{datetime.now().strftime('%Y%m%d')}_{str(i).zfill(6)}",
                'sender_phone': sender,
                'receiver_phone': receiver,
                'transaction_type': trans_type,
                'amount': amount,
                'fee': fee,
                'transaction_date': date,
                'location': location,
                'currency': 'KES',
                'status': 'COMPLETED' if random.random() < 0.98 else 'FAILED',  # 98% success rate
                'fraud_risk_score': fraud_risk,
                'merchant_id': f"MERCHANT_{random.randint(1000, 9999)}" if trans_type in ['MERCHANT_PAYMENT', 'BILL_PAYMENT'] else None,
                'reference_number': self.fake.uuid4(),
                'channel': random.choice(['WEB', 'USSD', 'APP']),
                'category': self.categorize_transaction(trans_type)
            }
            transactions.append(transaction)
        
        logger.info(f"Generated {len(transactions)} transactions successfully!")
        return pd.DataFrame(transactions)
    
    def categorize_transaction(self, transaction_type):
        """Categorize transactions by business type"""
        categories = {
            'P2P_TRANSFER': 'Person-to-Person',
            'MERCHANT_PAYMENT': 'Business Payments',
            'BILL_PAYMENT': 'Bills & Utilities',
            'AIRTIME_TOPUP': 'Airtime & Data',
            'WITHDRAWAL': 'Cash Out',
            'DEPOSIT': 'Cash In'
        }
        return categories.get(transaction_type, 'Other')

if __name__ == "__main__":
    generator = MPesaDataGenerator(num_transactions=15000)
    df = generator.generate_transactions()
    
    # Save to CSV
    df.to_csv('data/raw_mpesa_transactions.csv', index=False)
    logger.info("Data saved to data/raw_mpesa_transactions.csv")
    
    # Display sample
    print(df.head())
    print(f"\nDataset shape: {df.shape}")
    print(f"\nTransaction types distribution:\n{df['transaction_type'].value_counts()}")
    print(f"\nFraud risk score statistics:\n{df['fraud_risk_score'].describe()}")