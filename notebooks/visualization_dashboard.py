import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

load_dotenv()

# Setup plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class MPesaAnalyticsDashboard:
    def __init__(self):
        # Use SQLite instead of PostgreSQL
        db_type = os.getenv('DB_TYPE', 'sqlite')
        if db_type == 'sqlite':
            self.engine = create_engine(f"sqlite:///data/{os.getenv('DB_NAME', 'mpesa_analytics.db')}")
        else:
            # PostgreSQL connection (for production)
            self.engine = create_engine(
                f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
                f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )

    def load_analytics_data(self):
        """Load analytics data from database"""
        query = """
        SELECT
            t.*,
            d.total_transactions as daily_total,
            d.total_amount as daily_amount,
            d.fraud_attempts as daily_fraud_attempts
        FROM transformed_transactions t
        LEFT JOIN daily_transaction_summary d ON t.date_part_date = d.summary_date
        WHERE t.transaction_date >= date('now', '-30 days')
        """
        return pd.read_sql(query, self.engine)

    def create_transaction_trends_plot(self, df):
        """Create transaction trends plot"""
        plt.figure(figsize=(15, 10))

        # Subplot 1: Daily transaction volume
        plt.subplot(2, 2, 1)
        daily_vol = df.groupby('date_part_date')['amount'].sum().reset_index()
        plt.plot(daily_vol['date_part_date'], daily_vol['amount'], marker='o', linewidth=2)
        plt.title('Daily Transaction Volume (KES)', fontsize=14, fontweight='bold')
        plt.xlabel('Date')
        plt.ylabel('Total Amount (KES)')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)

        # Subplot 2: Transaction count by type
        plt.subplot(2, 2, 2)
        type_counts = df['transaction_type'].value_counts()
        plt.pie(type_counts.values, labels=type_counts.index, autopct='%1.1f%%', startangle=90)
        plt.title('Transaction Type Distribution', fontsize=14, fontweight='bold')

        # Subplot 3: Hourly transaction pattern
        plt.subplot(2, 2, 3)
        hourly_counts = df.groupby('hour_of_day').size()
        plt.bar(hourly_counts.index, hourly_counts.values, color='skyblue')
        plt.title('Hourly Transaction Patterns', fontsize=14, fontweight='bold')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Transactions')

        # Subplot 4: Fraud risk distribution
        plt.subplot(2, 2, 4)
        fraud_dist = df['fraud_category'].value_counts()
        colors = ['green' if x == 'Low Risk' else 'orange' if x == 'Medium Risk' else 'red' for x in fraud_dist.index]
        plt.bar(fraud_dist.index, fraud_dist.values, color=colors)
        plt.title('Fraud Risk Category Distribution', fontsize=14, fontweight='bold')
        plt.ylabel('Count')
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig('reports/transaction_analytics_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_fraud_detection_visuals(self, df):
        """Create fraud detection related visuals"""
        plt.figure(figsize=(15, 8))

        # Fraud attempts over time
        plt.subplot(1, 2, 1)
        fraud_by_date = df[df['fraud_risk_score'] > 70].groupby('date_part_date').size()
        plt.plot(fraud_by_date.index, fraud_by_date.values, marker='o', color='red', linewidth=2)
        plt.title('High-Risk Transactions Over Time', fontsize=14, fontweight='bold')
        plt.xlabel('Date')
        plt.ylabel('Number of High-Risk Transactions')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)

        # Amount vs Fraud Risk Score
        plt.subplot(1, 2, 2)
        scatter = plt.scatter(df['amount'], df['fraud_risk_score'],
                            c=df['fraud_risk_score'], cmap='viridis', alpha=0.6)
        plt.colorbar(scatter, label='Fraud Risk Score')
        plt.title('Transaction Amount vs Fraud Risk Score', fontsize=14, fontweight='bold')
        plt.xlabel('Amount (KES)')
        plt.ylabel('Fraud Risk Score')

        plt.tight_layout()
        plt.savefig('reports/fraud_detection_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()

    def generate_user_segmentation_charts(self, df):
        """Generate user segmentation charts"""
        plt.figure(figsize=(15, 10))

        # Volume category distribution
        plt.subplot(2, 2, 1)
        vol_cat_counts = df['transaction_volume_category'].value_counts()
        plt.bar(vol_cat_counts.index, vol_cat_counts.values)
        plt.title('Transaction Volume Categories', fontsize=14, fontweight='bold')
        plt.ylabel('Count')
        plt.xticks(rotation=45)

        # Location-based analysis
        plt.subplot(2, 2, 2)
        loc_counts = df['location'].value_counts().head(10)  # Top 10 locations
        plt.bar(loc_counts.index, loc_counts.values)
        plt.title('Top 10 Transaction Locations', fontsize=14, fontweight='bold')
        plt.ylabel('Number of Transactions')
        plt.xticks(rotation=45)

        # Channel usage
        plt.subplot(2, 2, 3)
        channel_dist = df['channel'].value_counts()
        plt.pie(channel_dist.values, labels=channel_dist.index, autopct='%1.1f%%')
        plt.title('Transaction Channel Distribution', fontsize=14, fontweight='bold')

        # Amount distribution by category
        plt.subplot(2, 2, 4)
        df.boxplot(column='amount', by='category', ax=plt.gca())
        plt.title('Transaction Amount Distribution by Category', fontsize=14, fontweight='bold')
        plt.suptitle('')  # Remove auto title
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig('reports/user_segmentation_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_comprehensive_dashboard(self):
        """Create comprehensive dashboard"""
        print("Loading analytics data...")
        df = self.load_analytics_data()

        print("Creating transaction trends dashboard...")
        self.create_transaction_trends_plot(df)

        print("Creating fraud detection visuals...")
        self.create_fraud_detection_visuals(df)

        print("Creating user segmentation charts...")
        self.generate_user_segmentation_charts(df)

        # Generate summary statistics
        print("\n" + "="*60)
        print("MPESA TRANSACTION ANALYTICS SUMMARY")
        print("="*60)
        print(f"Total Transactions: {len(df):,}")
        print(f"Total Transaction Value: KES {df['amount'].sum():,.2f}")
        print(f"Average Transaction Value: KES {df['amount'].mean():.2f}")
        print(f"Highest Transaction: KES {df['amount'].max():,.2f}")
        print(f"Most Active Hour: {df['hour_of_day'].mode()[0]}:00-{df['hour_of_day'].mode()[0]+1}:00")
        print(f"Busiest Location: {df['location'].mode()[0]}")
        print(f"Transaction Types: {df['transaction_type'].nunique()}")
        print(f"High-Risk Transactions: {(df['fraud_risk_score'] > 70).sum()}")
        print("="*60)

if __name__ == "__main__":
    # Create reports directory if it doesn't exist
    import os
    os.makedirs('reports', exist_ok=True)

    # Generate dashboard
    dashboard = MPesaAnalyticsDashboard()
    dashboard.create_comprehensive_dashboard()
    print("\nDashboard reports generated successfully in 'reports/' directory!")