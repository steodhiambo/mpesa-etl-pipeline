from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from scripts.main_pipeline import run_mpesa_etl_pipeline
import os

# Define the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [os.getenv('AIRFLOW_ADMIN_EMAIL', 'admin@example.com')],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'mpesa_transaction_analytics_pipeline',
    default_args=default_args,
    description='ETL pipeline for M-Pesa transaction analytics with enhanced monitoring',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    tags=['mpesa', 'etl', 'analytics', 'transactions', 'production'],
    doc_md="""# M-Pesa ETL Pipeline
    
    This DAG processes M-Pesa transaction data through:
    1. Data extraction from source systems
    2. Data validation and quality checks
    3. Data transformation and enrichment
    4. Loading to analytics database
    5. Report generation and alerting
    """
)

# Define tasks
def run_etl_callable(**context):
    """Callable function to run the ETL pipeline with context"""
    try:
        run_mpesa_etl_pipeline()
        return "ETL pipeline completed successfully"
    except Exception as e:
        context['task_instance'].log.error(f"ETL pipeline failed: {str(e)}")
        raise

def send_failure_notification(context):
    """Send notification on task failure"""
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    subject = f"Airflow Alert: {task_instance.task_id} Failed"
    body = f"""
    Task: {task_instance.task_id}
    DAG: {task_instance.dag_id}
    Execution Date: {dag_run.execution_date}
    Log URL: {task_instance.log_url}
    """
    
    # Log the failure (email would be configured separately)
    task_instance.log.error(f"Task failed: {subject}\n{body}")

# Task 1: Data Extraction and Processing
extract_task = PythonOperator(
    task_id='extract_and_process_transactions',
    python_callable=run_etl_callable,
    on_failure_callback=send_failure_notification,
    dag=dag,
    doc_md="Extract M-Pesa transactions and process through ETL pipeline"
)

# Task 2: Data Validation
validate_task = BashOperator(
    task_id='validate_data_quality',
    bash_command="""
    cd /opt/airflow/dags &&
    python -c "
    import pandas as pd
    from sqlalchemy import create_engine
    import os
    
    engine = create_engine(os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'])
    
    # Check latest processed data
    result = pd.read_sql('''
        SELECT 
            COUNT(*) as total_records,
            MIN(transaction_date) as earliest_date,
            MAX(transaction_date) as latest_date
        FROM transformed_transactions
        WHERE load_timestamp >= NOW() - INTERVAL '2 hours'
    ''', engine)
    
    print('Latest data validation:')
    print(result)
    
    # Basic quality checks
    if result.iloc[0]['total_records'] == 0:
        raise ValueError('No new records found in last 2 hours')
    "
    """,
    dag=dag,
)

# Task 3: Generate Reports
report_task = BashOperator(
    task_id='generate_daily_report',
    bash_command="""
    cd /opt/airflow/dags &&
    python -c "
    import pandas as pd
    from sqlalchemy import create_engine
    import os
    from datetime import datetime
    
    engine = create_engine(os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'])
    
    # Generate today's summary
    query = '''
        SELECT 
            summary_date,
            total_transactions,
            total_amount,
            fraud_attempts,
            successful_transactions,
            failed_transactions
        FROM daily_transaction_summary
        WHERE summary_date = CURRENT_DATE
        ORDER BY summary_date DESC
        LIMIT 1
    '''
    
    summary = pd.read_sql(query, engine)
    
    if not summary.empty:
        print(f'=== DAILY TRANSACTION SUMMARY ===')
        print(f'Date: {summary.iloc[0][\"summary_date\"]}')
        print(f'Total Transactions: {summary.iloc[0][\"total_transactions\"]:,}')
        print(f'Total Amount: KES {summary.iloc[0][\"total_amount\"]:,.2f}')
        print(f'Successful: {summary.iloc[0][\"successful_transactions\"]:,}')
        print(f'Failed: {summary.iloc[0][\"failed_transactions\"]:,}')
        print(f'Potential Fraud Attempts: {summary.iloc[0][\"fraud_attempts\"]:,}')
        print('=' * 35)
    else:
        print('No data available for today')
    "
    """,
    dag=dag,
)

# Task 4: Cleanup (optional)
cleanup_task = BashOperator(
    task_id='cleanup_old_logs',
    bash_command='find /opt/airflow/logs -name "*.log*" -mtime +7 -delete || true',
    dag=dag,
)

# Define task dependencies
extract_task >> validate_task >> report_task >> cleanup_task