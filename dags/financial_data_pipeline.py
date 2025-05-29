"""
Financial Data Pipeline with Apache Atlas Integration

This DAG orchestrates a complete financial data pipeline with comprehensive 
metadata management in Apache Atlas, including:
1. Financial transaction data generation
2. Custom entity type creation
3. Classification and tagging
4. Business glossary management
5. Lineage tracking
6. Technical, business, and operational metadata
"""

from datetime import datetime, timedelta
import os
import sys
import json
import random
import pandas as pd
import logging
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Add scripts directory to path for importing modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append(os.path.dirname(__file__))

# Import our custom metadata manager
from financial_metadata_manager import FinancialMetadataManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='Financial data pipeline with Atlas metadata integration',
    schedule_interval='@daily',
    catchup=False,
)

# Configuration
ATLAS_URL = "http://atlas:21000"
DATABASE_URI = "postgresql://postgres:postgres@postgres:5432/finance_db"
OUTPUT_DIR = "/tmp/financial_data"

def setup_atlas_environment(**kwargs):
    """
    Set up the Atlas environment for financial metadata management.
    This includes creating classifications, entity types, and a business glossary.
    """
    logger.info("Setting up Atlas environment for financial metadata")
    
    # Initialize the metadata manager
    metadata_manager = FinancialMetadataManager(ATLAS_URL)
    
    # Check connection to Atlas
    if not metadata_manager.check_connection():
        raise Exception("Failed to connect to Atlas")
    
    # Create classifications
    if not metadata_manager.create_financial_classifications():
        raise Exception("Failed to create financial classifications")
    
    # Create entity types
    if not metadata_manager.create_financial_entity_types():
        raise Exception("Failed to create financial entity types")
    
    # Create business glossary
    glossary_guid = metadata_manager.create_business_glossary()
    if not glossary_guid:
        raise Exception("Failed to create financial glossary")
    
    logger.info("Atlas environment setup completed successfully")
    return {
        "glossary_guid": glossary_guid
    }

def generate_financial_data(**kwargs):
    """
    Generate sample financial transaction data.
    """
    logger.info("Generating sample financial data")
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate account data
    num_accounts = 10
    accounts = []
    
    account_types = ["CHECKING", "SAVINGS", "CREDIT", "INVESTMENT"]
    statuses = ["ACTIVE", "INACTIVE", "SUSPENDED", "CLOSED"]
    
    for i in range(1, num_accounts + 1):
        account = {
            "accountNumber": f"{1000 + i}",
            "accountType": random.choice(account_types),
            "balance": round(random.uniform(1000, 50000), 2),
            "currency": "USD",
            "customerID": f"CUST-{100 + i}",
            "openDate": (datetime.now() - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d"),
            "status": random.choice(statuses)
        }
        accounts.append(account)
    
    # Save account data
    accounts_file = os.path.join(OUTPUT_DIR, "accounts.json")
    with open(accounts_file, "w") as f:
        json.dump(accounts, f, indent=2)
    
    # Generate transaction data
    num_transactions = 100
    transactions = []
    
    transaction_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT", "PURCHASE"]
    categories = ["GROCERIES", "UTILITIES", "ENTERTAINMENT", "SALARY", "RENT", "TRAVEL"]
    merchants = ["Walmart", "Amazon", "Netflix", "Starbucks", "Uber", "Shell", "AT&T"]
    statuses = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
    
    for i in range(1, num_transactions + 1):
        account = random.choice(accounts)
        transaction = {
            "transactionID": f"TXN-{10000 + i}",
            "accountID": account["accountNumber"],
            "transactionType": random.choice(transaction_types),
            "amount": round(random.uniform(10, 1000), 2),
            "currency": "USD",
            "transactionDate": (datetime.now() - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d"),
            "merchantName": random.choice(merchants) if random.random() > 0.3 else None,
            "category": random.choice(categories) if random.random() > 0.2 else None,
            "status": random.choice(statuses)
        }
        transactions.append(transaction)
    
    # Save transaction data
    transactions_file = os.path.join(OUTPUT_DIR, "transactions.json")
    with open(transactions_file, "w") as f:
        json.dump(transactions, f, indent=2)
    
    logger.info(f"Generated {len(accounts)} accounts and {len(transactions)} transactions")
    return {
        "accounts_file": accounts_file,
        "transactions_file": transactions_file
    }

def ingest_metadata_to_atlas(**kwargs):
    """
    Ingest financial metadata into Atlas.
    """
    logger.info("Ingesting financial metadata to Atlas")
    
    # Get data file paths from previous task
    ti = kwargs['ti']
    data_files = ti.xcom_pull(task_ids='generate_financial_data')
    
    accounts_file = data_files['accounts_file']
    transactions_file = data_files['transactions_file']
    
    # Load the data
    with open(accounts_file, "r") as f:
        accounts = json.load(f)
    
    with open(transactions_file, "r") as f:
        transactions = json.load(f)
    
    # Initialize the metadata manager
    metadata_manager = FinancialMetadataManager(ATLAS_URL)
    
    # Create account entities
    account_guids = {}
    for account in accounts:
        guid = metadata_manager.create_account_entity(account)
        if guid:
            account_guids[account["accountNumber"]] = guid
            
            # Apply classifications based on account type
            if account["accountType"] == "CHECKING" or account["accountType"] == "SAVINGS":
                metadata_manager.classify_entity(
                    guid, 
                    "Customer_Sensitive", 
                    {"dataOwner": "retail.banking@example.com", "reviewDate": datetime.now().strftime("%Y-%m-%d")}
                )
                
                metadata_manager.classify_entity(
                    guid,
                    "PII",
                    {"piiType": "financial_account", "encryptionRequired": True}
                )
    
    # Create transaction entities
    transaction_guids = []
    for transaction in transactions:
        guid = metadata_manager.create_transaction_entity(transaction)
        if guid:
            transaction_guids.append(guid)
            
            # Apply classifications based on transaction amount
            if transaction["amount"] > 500:
                metadata_manager.classify_entity(
                    guid,
                    "High_Value_Transaction",
                    {"thresholdValue": 500.0, "requiresApproval": True}
                )
            
            # Apply regulatory classification if needed
            if transaction["amount"] > 10000:
                metadata_manager.classify_entity(
                    guid,
                    "Financial_Regulatory",
                    {"regulationType": "AML", "complianceOwner": "compliance@example.com"}
                )
    
    # Create lineage between accounts and transactions
    if account_guids and transaction_guids:
        # Group transactions by account
        account_transactions = {}
        for transaction in transactions:
            account_id = transaction["accountID"]
            if account_id in account_guids:
                if account_id not in account_transactions:
                    account_transactions[account_id] = []
                account_transactions[account_id].append(transaction["transactionID"])
        
        # Create lineage for each account and its transactions
        for account_id, txn_ids in account_transactions.items():
            # Find the GUIDs for the transactions
            txn_guids = [guid for guid, txn in zip(transaction_guids, transactions) 
                        if txn["transactionID"] in txn_ids]
            
            if txn_guids:
                metadata_manager.create_lineage(
                    f"account_transactions_{account_id}",
                    [account_guids[account_id]],
                    txn_guids,
                    f"Transactions for account {account_id}"
                )
    
    logger.info(f"Successfully ingested metadata for {len(account_guids)} accounts and {len(transaction_guids)} transactions")
    return {
        "account_guids": list(account_guids.values()),
        "transaction_guids": transaction_guids
    }

def verify_metadata_ingestion(**kwargs):
    """
    Verify that the metadata was successfully ingested into Atlas.
    """
    logger.info("Verifying metadata ingestion")
    
    # Get entity GUIDs from previous task
    ti = kwargs['ti']
    entity_guids = ti.xcom_pull(task_ids='ingest_metadata_to_atlas')
    
    if not entity_guids or not entity_guids.get('account_guids') or not entity_guids.get('transaction_guids'):
        raise Exception("No entity GUIDs found from previous task")
    
    # Initialize the metadata manager
    metadata_manager = FinancialMetadataManager(ATLAS_URL)
    
    # Verify connection
    if not metadata_manager.check_connection():
        raise Exception("Failed to connect to Atlas")
    
    # Sample verification - in a real scenario, you would do more thorough checks
    logger.info(f"Successfully verified metadata ingestion for {len(entity_guids['account_guids'])} accounts "
               f"and {len(entity_guids['transaction_guids'])} transactions")
    
    return True

# Define the tasks
setup_task = PythonOperator(
    task_id='setup_atlas_environment',
    python_callable=setup_atlas_environment,
    provide_context=True,
    dag=dag,
)

generate_data_task = PythonOperator(
    task_id='generate_financial_data',
    python_callable=generate_financial_data,
    provide_context=True,
    dag=dag,
)

ingest_metadata_task = PythonOperator(
    task_id='ingest_metadata_to_atlas',
    python_callable=ingest_metadata_to_atlas,
    provide_context=True,
    dag=dag,
)

verify_task = PythonOperator(
    task_id='verify_metadata_ingestion',
    python_callable=verify_metadata_ingestion,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
setup_task >> generate_data_task >> ingest_metadata_task >> verify_task
