import pytest
from src.ingestion.metadata_ingestion import MetadataIngestion
import os
import yaml

@pytest.fixture
def config_file(tmp_path):
    """Create a temporary config file for testing."""
    config = {
        'atlas': {
            'server': {
                'url': 'http://localhost:21000',
                'authentication': {
                    'type': 'basic',
                    'username': 'test_user'
                }
            },
            'logging': {
                'level': 'DEBUG',
                'file': 'test.log'
            }
        }
    }
    config_path = tmp_path / "test_config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(config, f)
    return str(config_path)

@pytest.fixture
def ingestion(config_file):
    """Create a MetadataIngestion instance for testing."""
    os.environ['ATLAS_PASSWORD'] = 'test_password'
    return MetadataIngestion(config_path=config_file)

def test_init(ingestion):
    """Test initialization of MetadataIngestion."""
    assert ingestion.config is not None
    assert ingestion.session is not None
    assert ingestion.logger is not None

def test_create_entity(ingestion, mocker):
    """Test entity creation."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"guid": "test-guid"}
    mock_response.raise_for_status = mocker.Mock()
    
    mocker.patch('requests.Session.post', return_value=mock_response)
    
    entity_data = {
        "typeName": "database",
        "attributes": {
            "name": "test_db",
            "description": "Test database"
        }
    }
    
    result = ingestion.create_entity("database", entity_data)
    assert result["guid"] == "test-guid"

def test_get_entity(ingestion, mocker):
    """Test entity retrieval."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"guid": "test-guid", "name": "test_db"}
    mock_response.raise_for_status = mocker.Mock()
    
    mocker.patch('requests.Session.get', return_value=mock_response)
    
    result = ingestion.get_entity("test-guid")
    assert result["guid"] == "test-guid"
    assert result["name"] == "test_db"

def test_update_entity(ingestion, mocker):
    """Test entity update."""
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"guid": "test-guid", "name": "updated_db"}
    mock_response.raise_for_status = mocker.Mock()
    
    mocker.patch('requests.Session.put', return_value=mock_response)
    
    entity_data = {
        "typeName": "database",
        "attributes": {
            "name": "updated_db",
            "description": "Updated database"
        }
    }
    
    result = ingestion.update_entity("test-guid", entity_data)
    assert result["guid"] == "test-guid"
    assert result["name"] == "updated_db"

def test_delete_entity(ingestion, mocker):
    """Test entity deletion."""
    mock_response = mocker.Mock()
    mock_response.raise_for_status = mocker.Mock()
    
    mocker.patch('requests.Session.delete', return_value=mock_response)
    
    ingestion.delete_entity("test-guid")
    # If no exception is raised, the test passes 