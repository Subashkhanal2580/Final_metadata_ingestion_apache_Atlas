#!/usr/bin/env python3
"""
Apache Atlas Metadata Ingestion Module
This module handles the ingestion of metadata into Apache Atlas.
"""

import logging
import yaml
import os
from typing import Dict, List, Any
import requests
from requests.auth import HTTPBasicAuth

class MetadataIngestion:
    def __init__(self, config_path: str = "../config/atlas-config.yaml"):
        """Initialize the metadata ingestion client."""
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.session = self._create_session()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"Failed to load configuration: {str(e)}")

    def _setup_logging(self):
        """Configure logging based on settings."""
        log_config = self.config['atlas']['logging']
        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filename=log_config['file']
        )
        self.logger = logging.getLogger(__name__)

    def _create_session(self) -> requests.Session:
        """Create an authenticated session for Atlas API."""
        session = requests.Session()
        auth_config = self.config['atlas']['server']['authentication']
        session.auth = HTTPBasicAuth(
            auth_config['username'],
            os.getenv('ATLAS_PASSWORD', '')
        )
        return session

    def create_entity(self, entity_type: str, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new entity in Atlas."""
        try:
            url = f"{self.config['atlas']['server']['url']}/api/atlas/v2/entity"
            response = self.session.post(url, json=entity_data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to create entity: {str(e)}")
            raise

    def update_entity(self, entity_guid: str, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing entity in Atlas."""
        try:
            url = f"{self.config['atlas']['server']['url']}/api/atlas/v2/entity/guid/{entity_guid}"
            response = self.session.put(url, json=entity_data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to update entity: {str(e)}")
            raise

    def get_entity(self, entity_guid: str) -> Dict[str, Any]:
        """Retrieve an entity from Atlas."""
        try:
            url = f"{self.config['atlas']['server']['url']}/api/atlas/v2/entity/guid/{entity_guid}"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get entity: {str(e)}")
            raise

    def delete_entity(self, entity_guid: str) -> None:
        """Delete an entity from Atlas."""
        try:
            url = f"{self.config['atlas']['server']['url']}/api/atlas/v2/entity/guid/{entity_guid}"
            response = self.session.delete(url)
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to delete entity: {str(e)}")
            raise

def main():
    """Main entry point for the metadata ingestion process."""
    try:
        ingestion = MetadataIngestion()
        # Example usage
        entity_data = {
            "typeName": "database",
            "attributes": {
                "name": "example_db",
                "description": "Example database"
            }
        }
        result = ingestion.create_entity("database", entity_data)
        print(f"Created entity: {result}")
    except Exception as e:
        logging.error(f"Metadata ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 