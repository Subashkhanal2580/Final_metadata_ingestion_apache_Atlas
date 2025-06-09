#!/usr/bin/env python
"""
Create Financial Classifications for Apache Atlas

This script creates financial-specific classification types in Apache Atlas
to enhance metadata governance for financial data assets.
"""

import requests
import json
import sys
import logging
import argparse
from datetime import datetime
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AtlasClassificationManager:
    def __init__(self, atlas_url, username="admin", password="admin"):
        """Initialize Atlas client with connection details."""
        self.atlas_url = atlas_url.rstrip('/')
        self.auth = (username, password)
        self.headers = {"Content-Type": "application/json", "Accept": "application/json"}
        logger.info(f"Initializing Atlas classification manager for {atlas_url}")
    
    def check_connection(self):
        """Check if Atlas is running and return version info."""
        try:
            response = requests.get(f"{self.atlas_url}/api/atlas/admin/version", auth=self.auth)
            if response.status_code == 200:
                version_info = response.json()
                logger.info(f"Successfully connected to Atlas {version_info.get('Version', 'Unknown')}")
                return True
            else:
                logger.error(f"Atlas returned status code {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error connecting to Atlas: {e}")
            return False
    
    def create_financial_classifications(self):
        """Create financial-specific classification types in Atlas."""
        classifications = {
            "classificationDefs": [
                {
                    "category": "CLASSIFICATION",
                    "name": "Customer_Sensitive",
                    "description": "Contains sensitive customer financial information",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {"name": "dataOwner", "typeName": "string", "isOptional": True},
                        {"name": "reviewDate", "typeName": "date", "isOptional": True}
                    ]
                },
                {
                    "category": "CLASSIFICATION",
                    "name": "PII",
                    "description": "Contains personally identifiable information",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {"name": "piiType", "typeName": "string", "isOptional": False},
                        {"name": "encryptionRequired", "typeName": "boolean", "isOptional": True}
                    ]
                },
                {
                    "category": "CLASSIFICATION",
                    "name": "High_Value_Transaction",
                    "description": "Represents high-value financial transactions",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {"name": "thresholdValue", "typeName": "float", "isOptional": True},
                        {"name": "requiresApproval", "typeName": "boolean", "isOptional": True}
                    ]
                },
                {
                    "category": "CLASSIFICATION",
                    "name": "Financial_Regulatory",
                    "description": "Subject to financial regulations",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {"name": "regulationType", "typeName": "string", "isOptional": False},
                        {"name": "complianceOwner", "typeName": "string", "isOptional": True},
                        {"name": "lastReviewDate", "typeName": "date", "isOptional": True}
                    ]
                },
                {
                    "category": "CLASSIFICATION",
                    "name": "Fraud_Risk",
                    "description": "Indicates potential fraud risk",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {"name": "riskScore", "typeName": "float", "isOptional": False},
                        {"name": "riskLevel", "typeName": "string", "isOptional": False},
                        {"name": "detectionMethod", "typeName": "string", "isOptional": True}
                    ]
                }
            ]
        }
        
        try:
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/types/typedefs", 
                json=classifications,
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                created_types = [cls['name'] for cls in classifications['classificationDefs']]
                logger.info(f"Successfully created classifications: {', '.join(created_types)}")
                return True
            else:
                logger.error(f"Failed to create classifications: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error creating classifications: {e}")
            return False
    
    def apply_classification_to_entity(self, entity_guid, classification_name, attributes=None):
        """Apply a classification to an entity."""
        classification = {
            "typeName": classification_name
        }
        
        if attributes:
            classification["attributes"] = attributes
            
        try:
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity/guid/{entity_guid}/classifications",
                json=[classification],
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                logger.info(f"Applied classification {classification_name} to entity {entity_guid}")
                return True
            else:
                logger.error(f"Failed to apply classification: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error applying classification: {e}")
            return False

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Create financial classifications in Apache Atlas')
    parser.add_argument('--atlas-url', default='http://localhost:21000', help='Atlas server URL')
    parser.add_argument('--username', default='admin', help='Atlas username')
    parser.add_argument('--password', default='admin', help='Atlas password')
    
    args = parser.parse_args()
    
    # Initialize the Atlas client
    atlas_client = AtlasClassificationManager(args.atlas_url, args.username, args.password)
    
    # Check connection
    if not atlas_client.check_connection():
        logger.error("Failed to connect to Atlas. Exiting.")
        sys.exit(1)
    
    # Create financial classifications
    if atlas_client.create_financial_classifications():
        logger.info("Successfully created financial classifications")
    else:
        logger.error("Failed to create financial classifications")
        sys.exit(1)
    
    logger.info("Script completed successfully")

if __name__ == "__main__":
    main()
