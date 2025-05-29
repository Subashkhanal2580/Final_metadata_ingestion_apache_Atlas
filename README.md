# Apache Atlas Metadata Ingestion Workflow

This repository contains the implementation of a metadata ingestion workflow for Apache Atlas, enabling automated metadata collection and management for your data assets.

## Prerequisites

- Apache Atlas 2.3.0 or later
- Python 3.8 or later
- Java 8 or later
- Maven 3.6 or later
- Git

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/apache-atlas-metadata-ingestion.git
cd apache-atlas-metadata-ingestion
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Configure Apache Atlas:
   - Set up your `atlas-application.properties` file with the following configurations:
   ```properties
   atlas.server.http.port=21000
   atlas.server.http.address=0.0.0.0
   atlas.server.https.port=21443
   atlas.server.https.address=0.0.0.0
   ```

4. Configure the metadata ingestion:
   - Update the `config/atlas-config.yaml` file with your specific configurations
   - Set up your authentication credentials in `config/credentials.yaml`

## Implementation Details

### Directory Structure

```
apache-atlas-metadata-ingestion/
├── config/
│   ├── atlas-config.yaml
│   └── credentials.yaml
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── metadata_ingestion.py
│   │   └── utils.py
│   └── models/
│       ├── __init__.py
│       └── metadata_models.py
├── tests/
│   └── test_metadata_ingestion.py
├── requirements.txt
└── README.md
```

### Key Components

1. **Metadata Ingestion Module**
   - Handles the core functionality of metadata collection
   - Implements the Apache Atlas REST API integration
   - Manages metadata entity creation and updates

2. **Configuration Management**
   - Centralized configuration handling
   - Secure credential management
   - Environment-specific settings

3. **Utility Functions**
   - Helper functions for data transformation
   - Error handling and logging
   - API response processing

## Usage

1. Start Apache Atlas server:
```bash
./bin/atlas_start.py
```

2. Run the metadata ingestion:
```bash
python src/ingestion/metadata_ingestion.py
```

3. Monitor the ingestion process:
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
```

### Credentials Configuration

Store your credentials in `config/credentials.yaml`:

```yaml
credentials:
  username: your_username
  password: your_password
  api_key: your_api_key
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
- Contributors and maintainers 