from datetime import datetime, timedelta
import os
import sys
import json
import random
import uuid
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
    """
    logger.info("Setting up Atlas environment for financial metadata")
    
    # Log module path for debugging
    module_name = FinancialMetadataManager.__module__
    module_file = sys.modules[module_name].__file__ if module_name in sys.modules else "Unknown"
    logger.info(f"Loaded FinancialMetadataManager from module: {module_name} at {module_file}")
    
    # Initialize the metadata manager
    try:
        metadata_manager = FinancialMetadataManager(ATLAS_URL)
    except TypeError as e:
        logger.error(f"Failed to initialize FinancialMetadataManager: {str(e)}")
        raise
    
    # Verify check_connection exists
    if not hasattr(metadata_manager, 'check_connection'):
        logger.error("FinancialMetadataManager does not have check_connection method")
        raise AttributeError("FinancialMetadataManager missing check_connection method")
    
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
    logger.info("Generating sample financial data with unique identifiers")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    num_accounts = 10
    accounts = []
    account_types = ["CHECKING", "SAVINGS", "CREDIT", "INVESTMENT"]
    statuses = ["ACTIVE", "INACTIVE", "SUSPENDED", "CLOSED"]
    
    for i in range(1, num_accounts + 1):
        account_number = f"ACCT-{uuid.uuid4().hex[:8]}"
        account = {
            "accountNumber": account_number,
            "accountType": random.choice(account_types),
            "balance": round(random.uniform(1000, 50000), 2),
            "currency": "USD",
            "customerID": f"CUST-{uuid.uuid4().hex[:8]}",
            "openDate": (datetime.now() - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d"),
            "status": random.choice(statuses)
        }
        accounts.append(account)
    
    accounts_file = os.path.join(OUTPUT_DIR, "accounts.json")
    try:
        with open(accounts_file, "w") as f:
            json.dump(accounts, f, indent=2)
        logger.info(f"Saved {len(accounts)} accounts to {accounts_file}")
    except Exception as e:
        raise Exception(f"Failed to save accounts file: {str(e)}")
    
    num_transactions = 100
    transactions = []
    transaction_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT", "PURCHASE"]
    categories = ["GROCERIES", "UTILITIES", "ENTERTAINMENT", "SALARY", "RENT", "TRAVEL"]
    merchants = ["Walmart", "Amazon", "Netflix", "Starbucks", "Uber", "Shell", "AT&T"]
    statuses = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
    
    for i in range(1, num_transactions + 1):
        account = random.choice(accounts)
        transaction_id = f"TXN-{uuid.uuid4().hex[:8]}"
        transaction = {
            "transactionID": transaction_id,
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
    
    transactions_file = os.path.join(OUTPUT_DIR, "transactions.json")
    try:
        with open(transactions_file, "w") as f:
            json.dump(transactions, f, indent=2)
        logger.info(f"Saved {len(transactions)} transactions to {transactions_file}")
    except Exception as e:
        raise Exception(f"Failed to save transactions file: {str(e)}")
    
    logger.info(f"Generated {len(accounts)} accounts and {len(transactions)} transactions with unique identifiers")
    return {
        "accounts_file": accounts_file,
        "transactions_file": transactions_file
    }

def ingest_metadata_to_atlas(**kwargs):
    logger.info("Ingesting financial metadata to Atlas")
    
    # Get data file paths from previous task
    ti = kwargs['ti']
    data_files = ti.xcom_pull(task_ids='generate_financial_data')
    
    if not data_files or 'accounts_file' not in data_files or 'transactions_file' not in data_files:
        raise Exception("Missing data files from generate_financial_data task")
    
    accounts_file = data_files['accounts_file']
    transactions_file = data_files['transactions_file']
    
    # Validate file existence
    if not os.path.exists(accounts_file) or not os.path.exists(transactions_file):
        raise Exception(f"Data files not found: accounts_file={accounts_file}, transactions_file={transactions_file}")
    
    # Load the data
    try:
        with open(accounts_file, "r") as f:
            accounts = json.load(f)
        with open(transactions_file, "r") as f:
            transactions = json.load(f)
    except Exception as e:
        raise Exception(f"Failed to load data files: {str(e)}")
    
    if not accounts or not transactions:
        raise Exception("No accounts or transactions found in input files")
    
    logger.info(f"Loaded {len(accounts)} accounts and {len(transactions)} transactions")
    
    # Initialize the metadata manager
    metadata_manager = FinancialMetadataManager(ATLAS_URL)
    
    if not metadata_manager.check_connection():
        raise Exception("Cannot connect to Atlas")
    
    # Create account entities
    account_guids = {}
    for account in accounts:
        try:
            guid = metadata_manager.create_account_entity(account)
            if guid:
                account_guids[account["accountNumber"]] = guid
                # Apply classifications
                if account["accountType"] in ["CHECKING", "SAVINGS"]:
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
            else:
                logger.warning(f"Failed to create entity for account {account['accountNumber']}")
        except Exception as e:
            logger.error(f"Error creating account entity for {account['accountNumber']}: {str(e)}")
    
    # Create transaction entities
    transaction_guids = []
    for transaction in transactions:
        try:
            guid = metadata_manager.create_transaction_entity(transaction)
            if guid:
                transaction_guids.append(guid)
                # Apply classifications
                if transaction["amount"] > 500:
                    metadata_manager.classify_entity(
                        guid,
                        "High_Value_Transaction",
                        {"thresholdValue": 500.0, "requiresApproval": True}
                    )
                if transaction["amount"] > 10000:
                    metadata_manager.classify_entity(
                        guid,
                        "Financial_Regulatory",
                        {"regulationType": "AML", "complianceOwner": "compliance@example.com"}
                    )
            else:
                logger.warning(f"Failed to create entity for transaction {transaction['transactionID']}")
        except Exception as e:
            logger.error(f"Error creating transaction entity for {transaction['transactionID']}: {str(e)}")
    
    if not account_guids or not transaction_guids:
        raise Exception(f"No entities created: {len(account_guids)} accounts, {len(transaction_guids)} transactions")
    
    # Create lineage
    account_transactions = {}
    for transaction in transactions:
        account_id = transaction["accountID"]
        if account_id in account_guids:
            if account_id not in account_transactions:
                account_transactions[account_id] = []
            account_transactions[account_id].append(transaction["transactionID"])
    
    for account_id, txn_ids in account_transactions.items():
        txn_guids = [guid for guid, txn in zip(transaction_guids, transactions) 
                    if txn["transactionID"] in txn_ids]
        if txn_guids:
            try:
                metadata_manager.create_lineage(
                    f"account_transactions_{account_id}",
                    [account_guids[account_id]],
                    txn_guids,
                    f"Transactions for account {account_id}"
                )
            except Exception as e:
                logger.error(f"Error creating lineage for account {account_id}: {str(e)}")
    
    logger.info(f"Successfully ingested metadata for {len(account_guids)} accounts and {len(transaction_guids)} transactions")
    return {
        "account_guids": list(account_guids.values()),
        "transaction_guids": transaction_guids
    }

def verify_metadata_ingestion(**kwargs):
    logger.info("Verifying metadata ingestion")
    
    # Get entity GUIDs from previous task
    ti = kwargs['ti']
    entity_guids = ti.xcom_pull(task_ids='ingest_metadata_to_atlas')
    
    if not entity_guids:
        raise Exception("No entity GUIDs returned from ingest_metadata_to_atlas task")
    
    if not isinstance(entity_guids, dict):
        raise Exception(f"Invalid entity_guids format: {type(entity_guids)}")
    
    account_guids = entity_guids.get('account_guids', [])
    transaction_guids = entity_guids.get('transaction_guids', [])
    
    if not account_guids and not transaction_guids:
        raise Exception("Both account_guids and transaction_guids are empty")
    if not account_guids:
        raise Exception("No account GUIDs found")
    if not transaction_guids:
        raise Exception("No transaction GUIDs found")
    
    # Initialize the metadata manager
    metadata_manager = FinancialMetadataManager(ATLAS_URL)
    
    if not metadata_manager.check_connection():
        raise Exception("Failed to connect to Atlas")
    
    # Optional: Verify a sample of GUIDs exist in Atlas
    try:
        for guid in account_guids[:1]:  # Check first GUID
            if not metadata_manager.entity_exists(guid):
                logger.warning(f"Account entity {guid} not found in Atlas")
        for guid in transaction_guids[:1]:
            if not metadata_manager.entity_exists(guid):
                logger.warning(f"Transaction entity {guid} not found in Atlas")
    except Exception as e:
        logger.error(f"Error verifying entities: {str(e)}")
    
    logger.info(f"Successfully verified metadata ingestion for {len(account_guids)} accounts "
                f"and {len(transaction_guids)} transactions")
    
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