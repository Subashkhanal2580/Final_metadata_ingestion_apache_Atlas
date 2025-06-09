#!/usr/bin/env python
"""
Track Financial Data Lineage in Apache Atlas

This script creates and manages lineage relationships between financial data entities
in Apache Atlas, providing a complete view of data flow for financial transactions.
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

class AtlasLineageManager:
    def __init__(self, atlas_url, username="admin", password="admin"):
        """Initialize Atlas client with connection details."""
        self.atlas_url = atlas_url.rstrip('/')
        self.auth = (username, password)
        self.headers = {"Content-Type": "application/json", "Accept": "application/json"}
        logger.info(f"Initializing Atlas lineage manager for {atlas_url}")
    
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
    
    def get_entity_by_attribute(self, type_name, attribute_name, attribute_value):
        """Search for an entity by a specific attribute value."""
        query = {
            "typeName": type_name,
            "excludeDeletedEntities": True,
            "entityFilters": {
                "condition": "AND",
                "criterion": [
                    {
                        "attributeName": attribute_name,
                        "operator": "=",
                        "attributeValue": attribute_value
                    }
                ]
            }
        }
        
        try:
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/search/basic", 
                json=query,
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                entities = response.json().get("entities", [])
                if entities:
                    logger.info(f"Found {type_name} with {attribute_name}={attribute_value}")
                    return entities[0]
                else:
                    logger.info(f"No {type_name} found with {attribute_name}={attribute_value}")
                    return None
            else:
                logger.error(f"Search failed with status code {response.status_code}: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error searching for entity: {e}")
            return None
    
    def create_lineage_process(self, process_name, input_guids, output_guids, description=""):
        """Create a lineage process connecting input and output entities."""
        try:
            qualified_name = f"process-{process_name}-{int(time.time())}"
            
            process = {
                "entity": {
                    "typeName": "Process",
                    "attributes": {
                        "name": process_name,
                        "qualifiedName": qualified_name,
                        "description": description,
                        "inputs": [{"guid": guid} for guid in input_guids],
                        "outputs": [{"guid": guid} for guid in output_guids]
                    }
                }
            }
            
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=process,
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                guid = response.json().get("guidAssignments", {}).get(qualified_name)
                logger.info(f"Created lineage process with GUID: {guid}")
                return guid
            else:
                logger.error(f"Failed to create lineage: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error creating lineage: {e}")
            return None
    
    def get_lineage(self, entity_guid, depth=3, direction="BOTH"):
        """Get lineage information for an entity."""
        try:
            response = requests.get(
                f"{self.atlas_url}/api/atlas/v2/lineage/{entity_guid}",
                params={"depth": depth, "direction": direction},
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                lineage = response.json()
                logger.info(f"Retrieved lineage for entity {entity_guid}")
                return lineage
            else:
                logger.error(f"Failed to get lineage: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error getting lineage: {e}")
            return None
    
    def create_financial_data_flow(self):
        """Create a sample financial data flow with lineage."""
        try:
            # 1. Create source system entity (e.g., a payment processor system)
            source_system = {
                "entity": {
                    "typeName": "DataSet",
                    "attributes": {
                        "name": "Payment_Processor_System",
                        "qualifiedName": f"payment_processor_system_{int(time.time())}",
                        "description": "External payment processing system"
                    }
                }
            }
            
            source_response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=source_system,
                headers=self.headers,
                auth=self.auth
            )
            
            if source_response.status_code != 200:
                logger.error(f"Failed to create source system: {source_response.status_code} - {source_response.text}")
                return False
            
            source_guid = source_response.json().get("guidAssignments", {}).get(source_system["entity"]["attributes"]["qualifiedName"])
            
            # 2. Create raw transactions table
            raw_transactions = {
                "entity": {
                    "typeName": "hive_table",
                    "attributes": {
                        "name": "raw_financial_transactions",
                        "qualifiedName": f"raw_financial_transactions_{int(time.time())}",
                        "description": "Raw financial transaction data from payment processor",
                        "owner": "data_engineering",
                        "createTime": int(time.time() * 1000)
                    }
                }
            }
            
            raw_response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=raw_transactions,
                headers=self.headers,
                auth=self.auth
            )
            
            if raw_response.status_code != 200:
                logger.error(f"Failed to create raw transactions table: {raw_response.status_code} - {raw_response.text}")
                return False
            
            raw_guid = raw_response.json().get("guidAssignments", {}).get(raw_transactions["entity"]["attributes"]["qualifiedName"])
            
            # 3. Create enriched transactions table
            enriched_transactions = {
                "entity": {
                    "typeName": "hive_table",
                    "attributes": {
                        "name": "enriched_financial_transactions",
                        "qualifiedName": f"enriched_financial_transactions_{int(time.time())}",
                        "description": "Enriched financial transaction data with additional attributes",
                        "owner": "data_science",
                        "createTime": int(time.time() * 1000)
                    }
                }
            }
            
            enriched_response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=enriched_transactions,
                headers=self.headers,
                auth=self.auth
            )
            
            if enriched_response.status_code != 200:
                logger.error(f"Failed to create enriched transactions table: {enriched_response.status_code} - {enriched_response.text}")
                return False
            
            enriched_guid = enriched_response.json().get("guidAssignments", {}).get(enriched_transactions["entity"]["attributes"]["qualifiedName"])
            
            # 4. Create analytics table
            analytics_table = {
                "entity": {
                    "typeName": "hive_table",
                    "attributes": {
                        "name": "financial_analytics",
                        "qualifiedName": f"financial_analytics_{int(time.time())}",
                        "description": "Financial analytics data for reporting",
                        "owner": "business_intelligence",
                        "createTime": int(time.time() * 1000)
                    }
                }
            }
            
            analytics_response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=analytics_table,
                headers=self.headers,
                auth=self.auth
            )
            
            if analytics_response.status_code != 200:
                logger.error(f"Failed to create analytics table: {analytics_response.status_code} - {analytics_response.text}")
                return False
            
            analytics_guid = analytics_response.json().get("guidAssignments", {}).get(analytics_table["entity"]["attributes"]["qualifiedName"])
            
            # 5. Create lineage: Source System -> Raw Transactions (Extract)
            extract_process = self.create_lineage_process(
                "financial_data_extract",
                [source_guid],
                [raw_guid],
                "Extract financial transaction data from payment processor"
            )
            
            if not extract_process:
                logger.error("Failed to create extract process")
                return False
            
            # 6. Create lineage: Raw Transactions -> Enriched Transactions (Transform)
            transform_process = self.create_lineage_process(
                "financial_data_transform",
                [raw_guid],
                [enriched_guid],
                "Transform and enrich raw financial transaction data"
            )
            
            if not transform_process:
                logger.error("Failed to create transform process")
                return False
            
            # 7. Create lineage: Enriched Transactions -> Analytics Table (Load)
            load_process = self.create_lineage_process(
                "financial_data_load",
                [enriched_guid],
                [analytics_guid],
                "Load enriched financial data into analytics table"
            )
            
            if not load_process:
                logger.error("Failed to create load process")
                return False
            
            logger.info("Successfully created complete financial data flow with lineage")
            return True
            
        except Exception as e:
            logger.error(f"Error creating financial data flow: {e}")
            return False

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Track financial data lineage in Apache Atlas')
    parser.add_argument('--atlas-url', default='http://localhost:21000', help='Atlas server URL')
    parser.add_argument('--username', default='admin', help='Atlas username')
    parser.add_argument('--password', default='admin', help='Atlas password')
    
    args = parser.parse_args()
    
    # Initialize the Atlas client
    atlas_client = AtlasLineageManager(args.atlas_url, args.username, args.password)
    
    # Check connection
    if not atlas_client.check_connection():
        logger.error("Failed to connect to Atlas. Exiting.")
        sys.exit(1)
    
    # Create financial data flow with lineage
    if atlas_client.create_financial_data_flow():
        logger.info("Successfully created financial data flow with lineage")
    else:
        logger.error("Failed to create financial data flow")
        sys.exit(1)
    
    logger.info("Script completed successfully")

if __name__ == "__main__":
    main()
