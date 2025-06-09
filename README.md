# Financial Data Pipeline with Apache Atlas Integration

## Overview

This project implements an automated financial data processing pipeline using Apache Airflow, with a strong focus on metadata management and governance through Apache Atlas. The pipeline simulates the generation of financial data (accounts and transactions), ingests this data's metadata into Apache Atlas, and ensures data lineage and classification are properly recorded.

## Workflow Description

The primary workflow is orchestrated by the [financial_data_pipeline.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:0:0-0:0) DAG, which executes a sequence of tasks to process and catalog financial data. The core components and steps are:

1.  **Apache Atlas Environment Setup ([setup_atlas_environment](cci:1://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:50:0-93:5) task):**
    *   This initial step prepares the Apache Atlas instance for financial metadata.
    *   It leverages the [FinancialMetadataManager](cci:2://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:17:0-547:47) to:
        *   Establish a connection with the Atlas server.
        *   Define and create custom financial entity types (e.g., `fin_account`, `fin_transaction`) that model the structure of the financial data.
        *   Create relevant classifications (e.g., `PII`, `SensitiveData`, `FinancialData`) to tag data based on its nature and sensitivity.
        *   Set up a business glossary (e.g., `FinancialGlossary`) with terms specific to the financial domain (e.g., "Account Balance", "Transaction Type").

2.  **Financial Data Generation ([generate_financial_data](cci:1://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:95:0-161:5) task):**
    *   This task simulates the creation of raw financial data.
    *   It generates two main datasets:
        *   `accounts.json`: Contains a list of synthetic bank accounts with details like account number, type, balance, customer ID, open date, and status.
        *   `transactions.json`: Contains a list of synthetic financial transactions, including transaction ID, associated account ID, type, amount, date, merchant, category, and status.
    *   The generated JSON files are stored temporarily for the subsequent ingestion step.

3.  **Metadata Ingestion to Atlas ([ingest_metadata_to_atlas](cci:1://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:163:0-279:5) task):**
    *   This is a crucial step where the metadata of the generated financial data is registered in Apache Atlas.
    *   Using the [FinancialMetadataManager](cci:2://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:17:0-547:47), this task:
        *   Reads the `accounts.json` and `transactions.json` files.
        *   For each account and transaction:
            *   Creates corresponding entities in Atlas (of type `fin_account` and `fin_transaction`).
            *   Populates the attributes of these entities with the data from the JSON files.
            *   Establishes relationships between entities (e.g., linking a transaction entity to its corresponding account entity).
            *   Applies relevant classifications (e.g., marking account numbers as PII).
            *   Assigns business glossary terms to the entities and their attributes to provide business context.

4.  **Metadata Ingestion Verification ([verify_metadata_ingestion](cci:1://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:281:0-324:15) task):**
    *   The final step in the pipeline ensures that the metadata was successfully ingested into Atlas.
    *   It queries Atlas (via [FinancialMetadataManager](cci:2://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:17:0-547:47)) to:
        *   Search for the newly created account and transaction entities.
        *   Confirm their existence and potentially check if classifications and relationships were applied correctly.

## Core Scripts and Modules

*   **[dags/financial_data_pipeline.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:0:0-0:0):**
    *   The main Apache Airflow DAG file.
    *   Defines the tasks, their dependencies, and the overall workflow schedule.
    *   Imports and utilizes [FinancialMetadataManager](cci:2://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:17:0-547:47) for all Atlas interactions.

*   **[dags/financial_metadata_manager.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:0:0-0:0) :**
    *   A Python class ([FinancialMetadataManager](cci:2://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:17:0-547:47)) that encapsulates all logic for interacting with Apache Atlas.
    *   **Responsibilities:**
        *   Connecting to the Atlas REST API.
        *   Defining and creating type definitions (entity types, classifications, enums, structs).
        *   Creating, updating, and deleting entities in Atlas.
        *   Managing business glossary terms and categories.
        *   Handling classifications and their association with entities.
        *   Searching and retrieving metadata from Atlas.

## Implemented Features

*   **Automated Data Pipeline:** Uses Airflow for scheduled and repeatable execution of data processing tasks.
*   **Dynamic Metadata Modeling:** Defines custom types in Atlas to accurately represent financial concepts.
*   **Data Classification:** Applies classifications like PII and SensitiveData to entities for governance and compliance.
*   **Business Glossary Integration:** Links technical metadata with business terms for better understanding and context.
*   **Data Lineage (Implicit):** By creating relationships between accounts and transactions, basic lineage is established within Atlas.
*   **Sample Data Generation:** Includes a mechanism to produce synthetic data for pipeline demonstration and testing.
*   **Verification Step:** Ensures the integrity of the metadata ingestion process.
*   **Modular Design:** Separates Atlas interaction logic ([FinancialMetadataManager](cci:2://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:17:0-547:47)) from the pipeline orchestration logic ([financial_data_pipeline.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:0:0-0:0)), promoting reusability and maintainability.

## Configuration

*   **Atlas URL:** `http://atlas:21000` (configurable in [financial_data_pipeline.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:0:0-0:0))
*   **Database URI (Example):** `postgresql://postgres:postgres@postgres:5432/finance_db` (configurable, though its direct use in the provided pipeline snippet is for context, actual DB interaction for data storage isn't detailed in the DAG's Python functions beyond Atlas).
*   **Output Directory for Generated Data:** `/tmp/financial_data` (configurable in [financial_data_pipeline.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:0:0-0:0))

## How to Run

1.  Ensure Apache Airflow and Apache Atlas services are running and accessible.
2.  Configure the Atlas connection details (`ATLAS_URL`, username, password) within [financial_metadata_manager.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:0:0-0:0) (if hardcoded or passed) and [financial_data_pipeline.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:0:0-0:0).
3.  Place the [financial_data_pipeline.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_data_pipeline.py:0:0-0:0) DAG file in the Airflow DAGs folder.
4.  Ensure [financial_metadata_manager.py](cci:7://file:///d:/apache_atlas/yml_files/dags/financial_metadata_manager.py:0:0-0:0) is accessible in the Python path for the Airflow workers (e.g., in the same directory or a `scripts` subdirectory added to `sys.path`).
5.  Enable the `financial_data_pipeline` DAG in the Airflow UI and trigger a run.

## Potential Enhancements

*   Integration with a persistent data store (e.g., Hive, PostgreSQL) for the actual financial data, with Atlas capturing metadata for these stores.
*   More complex data transformations and lineage tracking.
*   Enhanced error handling and notifications.
*   Expansion of the business glossary and classification schemes.
*   Security hardening for Atlas credentials.
