"""
Financial Metadata Manager for Apache Atlas

This module provides a comprehensive interface for managing financial metadata in Apache Atlas,
including classifications, glossary terms, entities, and lineage tracking.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import requests
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FinancialMetadataManager:
    """
    A class to manage financial metadata in Apache Atlas.
    
    This class provides methods to create and manage financial data metadata,
    including classifications, glossary terms, entities, and lineage.
    """
    
    def __init__(self, atlas_url: str, username: str = "admin", password: str = "admin"):
        """
        Initialize the financial metadata manager.
        
        Args:
            atlas_url (str): URL of the Apache Atlas server (e.g., 'http://localhost:21000')
            username (str): Username for Atlas authentication
            password (str): Password for Atlas authentication
        """
        self.atlas_url = atlas_url.rstrip('/')
        self.auth = (username, password)
        self.headers = {"Content-Type": "application/json", "Accept": "application/json"}
        logger.info(f"Initializing Financial Metadata Manager for {atlas_url}")

    def check_connection(self) -> bool:
        """
        Check if Atlas is running and return version info.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
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

    def create_financial_classifications(self) -> bool:
        """
        Create standard classifications for financial data.
        
        Returns:
            bool: True if classifications were created successfully, False otherwise
        """
        try:
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
                    }
                ]
            }
            
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

    def create_business_glossary(self) -> Optional[str]:
        """
        Create a business glossary for financial terms.
        
        Returns:
            Optional[str]: GUID of the created glossary if successful, None otherwise
        """
        try:
            # Create the glossary
            glossary = {
                "name": "Financial_Glossary",
                "shortDescription": "Financial terms and definitions",
                "longDescription": "Comprehensive glossary of financial terms used in transaction processing",
                "language": "en"
            }
            
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/glossary",
                json=glossary,
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                glossary_guid = response.json().get("guid")
                logger.info(f"Created glossary with GUID: {glossary_guid}")
                
                # Create glossary terms
                terms = [
                    {
                        "name": "Transaction",
                        "shortDescription": "Financial transaction",
                        "longDescription": "A financial transaction between two parties involving the exchange of monetary value",
                        "examples": ["Purchase at a store", "Bank transfer"],
                        "anchor": {"glossaryGuid": glossary_guid}
                    },
                    {
                        "name": "Account",
                        "shortDescription": "Financial account",
                        "longDescription": "A financial arrangement that allows a customer to deposit, withdraw, and track their money",
                        "examples": ["Checking account", "Savings account"],
                        "anchor": {"glossaryGuid": glossary_guid}
                    },
                    {
                        "name": "Merchant",
                        "shortDescription": "Business accepting payments",
                        "longDescription": "A business or individual that accepts payments for goods or services",
                        "examples": ["Retail store", "Online shop"],
                        "anchor": {"glossaryGuid": glossary_guid}
                    }
                ]
                
                for term in terms:
                    term_response = requests.post(
                        f"{self.atlas_url}/api/atlas/v2/glossary/term",
                        json=term,
                        headers=self.headers,
                        auth=self.auth
                    )
                    
                    if term_response.status_code == 200:
                        logger.info(f"Created term: {term['name']}")
                    else:
                        logger.warning(f"Failed to create term {term['name']}: {term_response.status_code} - {term_response.text}")
                
                return glossary_guid
            else:
                logger.error(f"Failed to create glossary: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error creating glossary: {e}")
            return None

    def create_financial_entity_types(self) -> bool:
        """
        Create custom entity types for financial data.
        
        Returns:
            bool: True if entity types were created successfully, False otherwise
        """
        try:
            entity_types = {
                "entityDefs": [
                    {
                        "category": "ENTITY",
                        "name": "fin_account",
                        "description": "Financial account entity",
                        "typeVersion": "1.0",
                        "superTypes": ["DataSet"],
                        "attributeDefs": [
                            {"name": "accountNumber", "typeName": "string", "isOptional": False, "isUnique": True},
                            {"name": "accountType", "typeName": "string", "isOptional": False},
                            {"name": "balance", "typeName": "double", "isOptional": False},
                            {"name": "currency", "typeName": "string", "isOptional": False},
                            {"name": "customerID", "typeName": "string", "isOptional": False},
                            {"name": "openDate", "typeName": "date", "isOptional": False},
                            {"name": "status", "typeName": "string", "isOptional": False}
                        ]
                    },
                    {
                        "category": "ENTITY",
                        "name": "fin_transaction",
                        "description": "Financial transaction entity",
                        "typeVersion": "1.0",
                        "superTypes": ["DataSet"],
                        "attributeDefs": [
                            {"name": "transactionID", "typeName": "string", "isOptional": False, "isUnique": True},
                            {"name": "accountID", "typeName": "string", "isOptional": False},
                            {"name": "transactionType", "typeName": "string", "isOptional": False},
                            {"name": "amount", "typeName": "double", "isOptional": False},
                            {"name": "currency", "typeName": "string", "isOptional": False},
                            {"name": "transactionDate", "typeName": "date", "isOptional": False},
                            {"name": "merchantName", "typeName": "string", "isOptional": True},
                            {"name": "category", "typeName": "string", "isOptional": True},
                            {"name": "status", "typeName": "string", "isOptional": False}
                        ]
                    }
                ]
            }
            
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/types/typedefs", 
                json=entity_types,
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                created_types = [entity['name'] for entity in entity_types['entityDefs']]
                logger.info(f"Successfully created entity types: {', '.join(created_types)}")
                return True
            else:
                logger.error(f"Failed to create entity types: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error creating entity types: {e}")
            return False

    def create_account_entity(self, account_data: Dict) -> Optional[str]:
        """
        Create a financial account entity in Atlas.
        
        Args:
            account_data (Dict): Account data to create
            
        Returns:
            Optional[str]: GUID of the created entity if successful, None otherwise
        """
        try:
            qualified_name = f"account-{account_data['accountNumber']}"
            
            entity = {
                "entity": {
                    "typeName": "fin_account",
                    "attributes": {
                        "name": f"Account {account_data['accountNumber']}",
                        "qualifiedName": qualified_name,
                        "accountNumber": account_data['accountNumber'],
                        "accountType": account_data['accountType'],
                        "balance": account_data['balance'],
                        "currency": account_data['currency'],
                        "customerID": account_data['customerID'],
                        "openDate": account_data['openDate'],
                        "status": account_data['status']
                    }
                }
            }
            
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=entity,
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                guid = response.json().get("guidAssignments", {}).get(qualified_name)
                logger.info(f"Created account entity with GUID: {guid}")
                return guid
            else:
                logger.error(f"Failed to create account entity: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error creating account entity: {e}")
            return None

    def create_transaction_entity(self, transaction_data: Dict) -> Optional[str]:
        """
        Create a financial transaction entity in Atlas.
        
        Args:
            transaction_data (Dict): Transaction data to create
            
        Returns:
            Optional[str]: GUID of the created entity if successful, None otherwise
        """
        try:
            qualified_name = f"transaction-{transaction_data['transactionID']}"
            
            entity = {
                "entity": {
                    "typeName": "fin_transaction",
                    "attributes": {
                        "name": f"Transaction {transaction_data['transactionID']}",
                        "qualifiedName": qualified_name,
                        "transactionID": transaction_data['transactionID'],
                        "accountID": transaction_data['accountID'],
                        "transactionType": transaction_data['transactionType'],
                        "amount": transaction_data['amount'],
                        "currency": transaction_data['currency'],
                        "transactionDate": transaction_data['transactionDate'],
                        "merchantName": transaction_data.get('merchantName', ''),
                        "category": transaction_data.get('category', ''),
                        "status": transaction_data['status']
                    }
                }
            }
            
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=entity,
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code == 200:
                guid = response.json().get("guidAssignments", {}).get(qualified_name)
                logger.info(f"Created transaction entity with GUID: {guid}")
                return guid
            else:
                logger.error(f"Failed to create transaction entity: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error creating transaction entity: {e}")
            return None

    def classify_entity(self, entity_guid: str, classification_name: str, attributes: Dict = None) -> bool:
        """
        Apply a classification to an entity.
        
        Args:
            entity_guid (str): GUID of the entity to classify
            classification_name (str): Name of the classification to apply
            attributes (Dict, optional): Classification attributes
            
        Returns:
            bool: True if classification was successful, False otherwise
        """
        try:
            classification = {
                "typeName": classification_name
            }
            
            if attributes:
                classification["attributes"] = attributes
                
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

    def create_lineage(self, process_name: str, input_guids: List[str], output_guids: List[str], description: str = "") -> Optional[str]:
        """
        Create lineage between entities.
        
        Args:
            process_name (str): Name of the process
            input_guids (List[str]): List of input entity GUIDs
            output_guids (List[str]): List of output entity GUIDs
            description (str, optional): Process description
            
        Returns:
            Optional[str]: GUID of the created process if successful, None otherwise
        """
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
