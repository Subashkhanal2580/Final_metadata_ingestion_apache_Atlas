# Apache Atlas Metadata Ingestion Workflow

This repository contains the implementation of a metadata ingestion workflow for Apache Atlas, enabling automated metadata collection and management for your data assets. The implementation includes Apache Airflow DAGs for orchestration and automated metadata management.

## Prerequisites

### System Requirements
- Apache Atlas 2.3.0 or later
- Apache Airflow 2.5.0 or later
- Python 3.8 or later
- Java 8 or later
- Maven 3.6 or later
- Git

### Required Services
- Apache Atlas Server
- Apache Airflow Server
- Hive Metastore (for Hive metadata)
- PostgreSQL (for Airflow metadata)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/apache-atlas-metadata-ingestion.git
cd apache-atlas-metadata-ingestion
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install Python dependencies:
```bash
pip install -r requirements.txt
```

4. Configure Apache Atlas:
   - Set up your `atlas-application.properties` file with the following configurations:
   ```properties
   atlas.server.http.port=21000
   atlas.server.http.address=0.0.0.0
   atlas.server.https.port=21443
   atlas.server.https.address=0.0.0.0
   atlas.authentication.method.kerberos=false
   atlas.authentication.method.file=true
   atlas.authentication.method.file.filename=users-credentials.properties
   ```

5. Configure Airflow:
   - Set up your `airflow.cfg` with the following configurations:
   ```ini
   [core]
   dags_folder = /path/to/your/dags
   load_examples = False
   
   [webserver]
   web_server_port = 8080
   
   [scheduler]
   dag_file_processor_timeout = 600
   ```

6. Configure the metadata ingestion:
   - Update the `config/atlas-config.yaml` file with your specific configurations
   - Set up your authentication credentials in `config/credentials.yaml`

## DAG Workflow Implementation

### Directory Structure

```
apache-atlas-metadata-ingestion/
├── config/
│   ├── atlas-config.yaml
│   └── credentials.yaml
├── dags/
│   ├── financial_data_pipeline.py
│   └── financial_metadata_manager.py
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── metadata_ingestion.py
│   │   └── utils.py
│   └── models/
│       ├── __init__.py
│       └── metadata_models.py
├── scripts/
│   ├── create_financial_classifications.py
│   ├── create_financial_glossary.py
│   └── track_financial_lineage.py
├── tests/
│   └── test_metadata_ingestion.py
├── requirements.txt
└── README.md
```

### DAG Workflow Components

1. **Financial Data Pipeline DAG**
   - Handles the extraction, transformation, and loading of financial data
   - Integrates with Apache Atlas for metadata tracking
   - Implements data quality checks and validation

2. **Financial Metadata Manager DAG**
   - Manages metadata ingestion into Apache Atlas
   - Handles classification and glossary management
   - Tracks data lineage and relationships

### DAG Implementation

#### Financial Data Pipeline DAG
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.ingestion.metadata_ingestion import MetadataIngestion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='Financial data pipeline with metadata tracking',
    schedule_interval=timedelta(days=1),
)

def extract_data():
    # Implementation for data extraction
    pass

def transform_data():
    # Implementation for data transformation
    pass

def load_data():
    # Implementation for data loading
    pass

def track_metadata():
    ingestion = MetadataIngestion()
    # Implementation for metadata tracking
    pass

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

metadata_task = PythonOperator(
    task_id='track_metadata',
    python_callable=track_metadata,
    dag=dag,
)

extract_task >> transform_task >> load_task >> metadata_task
```

#### Financial Metadata Manager DAG
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.create_financial_classifications import create_classifications
from scripts.create_financial_glossary import create_glossary
from scripts.track_financial_lineage import track_lineage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'financial_metadata_manager',
    default_args=default_args,
    description='Financial metadata management workflow',
    schedule_interval=timedelta(days=1),
)

classifications_task = PythonOperator(
    task_id='create_classifications',
    python_callable=create_classifications,
    dag=dag,
)

glossary_task = PythonOperator(
    task_id='create_glossary',
    python_callable=create_glossary,
    dag=dag,
)

lineage_task = PythonOperator(
    task_id='track_lineage',
    python_callable=track_lineage,
    dag=dag,
)

classifications_task >> glossary_task >> lineage_task
```

## Integration Scripts

### Create Financial Classifications
```python
# scripts/create_financial_classifications.py
from src.ingestion.metadata_ingestion import MetadataIngestion

def create_classifications():
    ingestion = MetadataIngestion()
    classifications = [
        {
            "name": "PII",
            "description": "Personally Identifiable Information"
        },
        {
            "name": "Financial",
            "description": "Financial Data"
        }
    ]
    
    for classification in classifications:
        ingestion.create_classification(classification)
```

### Create Financial Glossary
```python
# scripts/create_financial_glossary.py
from src.ingestion.metadata_ingestion import MetadataIngestion

def create_glossary():
    ingestion = MetadataIngestion()
    glossary_terms = [
        {
            "name": "Revenue",
            "description": "Total income from business operations"
        },
        {
            "name": "Expense",
            "description": "Costs incurred in business operations"
        }
    ]
    
    for term in glossary_terms:
        ingestion.create_glossary_term(term)
```

### Track Financial Lineage
```python
# scripts/track_financial_lineage.py
from src.ingestion.metadata_ingestion import MetadataIngestion

def track_lineage():
    ingestion = MetadataIngestion()
    lineage_data = {
        "source": "raw_financial_data",
        "target": "processed_financial_data",
        "process": "financial_data_processing"
    }
    
    ingestion.create_lineage(lineage_data)
```

## Usage

1. Start Apache Atlas server:
```bash
./bin/atlas_start.py
```

2. Start Apache Airflow:
```bash
airflow webserver -p 8080
airflow scheduler
```

3. Run the metadata ingestion:
```bash
python src/ingestion/metadata_ingestion.py
```

4. Monitor the ingestion process:
```bash
tail -f logs/metadata_ingestion.log
```

## Configuration

### Atlas Configuration

Update the `config/atlas-config.yaml` file with your specific settings:

```yaml
atlas:
  server:
    url: http://localhost:21000
    authentication:
      type: basic
      username: admin
  metadata:
    namespace: default
    entity_types:
      - database
      - table
      - column
    ingestion:
      batch_size: 100
      retry_attempts: 3
      retry_delay: 5
  logging:
    level: INFO
    file: logs/metadata_ingestion.log
```

### Airflow Configuration

Create a `dags/config/airflow_config.yaml` file:

```yaml
airflow:
  dag_schedule: "0 0 * * *"  # Daily at midnight
  max_active_runs: 1
  catchup: false
  concurrency: 4
  email_on_failure: true
  email_on_retry: false
  retries: 3
  retry_delay: 300  # 5 minutes
```

## Testing

Run the test suite:
```bash
python -m pytest tests/
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

For support, please open an issue in the GitHub repository or contact the maintainers.

## Acknowledgments

- Apache Atlas Team
- Apache Airflow Team
- Contributors and maintainers 