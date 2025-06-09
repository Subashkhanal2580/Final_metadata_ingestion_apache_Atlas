from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import requests
import time
from requests.exceptions import RequestException
from dateutil.parser import parse as parse_date
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FinancialMetadataManager:
    def __init__(self, atlas_url: str, username: str = "admin", password: str = "admin"):
        """
        Initialize the financial metadata manager.
        
        Args:
            atlas_url (str): URL of the Apache Atlas server (e.g., 'http://atlas:21000')
            username (str): Username for Atlas authentication
            password (str): Password for Atlas authentication
        """
        self.atlas_url = atlas_url.rstrip('/')
        self.auth = (username, password)
        self.headers = {"Content-Type": "application/json", "Accept": "application/json"}
        logger.info(f"Initializing Financial Metadata Manager for {self.atlas_url}")

    def check_connection(self, retries: int = 3, delay: int = 5) -> bool:
        """
        Check if Atlas is running and return version info, with retries.
        
        Args:
            retries (int): Number of retry attempts for connection
            delay (int): Delay between retries in seconds
        
        Returns:
            bool: True if connection is successful, False otherwise
        
        Raises:
            RequestException: If connection fails after retries
        """
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(
                    f"{self.atlas_url}/api/atlas/admin/version",
                    auth=self.auth,
                    timeout=10
                )
                if response.status_code == 200:
                    version_info = response.json()
                    logger.info(f"Successfully connected to Atlas {version_info.get('Version', 'Unknown')}")
                    return True
                else:
                    logger.warning(f"Atlas connection attempt {attempt}/{retries} failed: {response.status_code}")
            except RequestException as e:
                logger.warning(f"Atlas connection attempt {attempt}/{retries} failed: {str(e)}")
            
            if attempt < retries:
                time.sleep(delay)
        
        logger.error("Failed to connect to Atlas after retries")
        raise RequestException("Unable to connect to Atlas after multiple attempts")

    def create_account_entity(self, account_data: Dict) -> str:
        """
        Create a financial account entity in Atlas.
        
        Args:
            account_data (Dict): Account data to create
        
        Returns:
            str: GUID of the created entity
        
        Raises:
            ValueError: If required fields are missing or invalid
            RequestException: If the API request fails
        """
        required_fields = ["accountNumber", "accountType", "balance", "currency", "customerID", "openDate", "status"]
        for field in required_fields:
            if field not in account_data or account_data[field] is None:
                raise ValueError(f"Missing or null required field: {field}")
        
        try:
            # Parse openDate and convert to milliseconds
            open_date = parse_date(account_data['openDate'])
            open_date_ms = int(open_date.timestamp() * 1000)  # Convert to milliseconds
        except ValueError as e:
            raise ValueError(f"Invalid openDate format: {account_data['openDate']}")
        
        try:
            qualified_name = f"account-{account_data['accountNumber']}"
            
            entity = {
                "entity": {
                    "typeName": "fin_account",
                    "attributes": {
                        "name": f"Account {account_data['accountNumber']}",
                        "qualifiedName": qualified_name,
                        "accountNumber": str(account_data['accountNumber']),
                        "accountType": str(account_data['accountType']),
                        "balance": float(account_data['balance']),
                        "currency": str(account_data['currency']),
                        "customerID": str(account_data['customerID']),
                        "openDate": open_date_ms,  # Use milliseconds
                        "status": str(account_data['status'])
                    }
                }
            }
            
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=entity,
                headers=self.headers,
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                response_json = response.json()
                guid_assignments = response_json.get("guidAssignments", {})
                guid = guid_assignments.get(qualified_name) or response_json.get("mutatedEntities", {}).get("CREATE", [{}])[0].get("guid")
                if guid:
                    logger.info(f"Created account entity with GUID: {guid}")
                    return guid
                else:
                    logger.error(f"No GUID found in response for account {qualified_name}")
                    raise RequestException("No GUID assigned for created account entity")
            elif response.status_code == 400 or (response.status_code == 404 and "ATLAS-404-00-007" in response.text):
                logger.error(f"Invalid account entity data: {response.text}")
                raise RequestException(f"Invalid account entity data: {response.text}")
            elif response.status_code == 401:
                logger.error("Authentication failed")
                raise RequestException("Atlas authentication failed")
            elif response.status_code == 409:
                logger.info(f"Account entity {qualified_name} already exists")
                search_response = requests.get(
                    f"{self.atlas_url}/api/atlas/v2/search/basic?typeName=fin_account&query=qualifiedName:{qualified_name}",
                    headers=self.headers,
                    auth=self.auth
                )
                if search_response.status_code == 200 and search_response.json().get("entities"):
                    return search_response.json()["entities"][0]["guid"]
                raise RequestException(f"Failed to retrieve existing account entity GUID")
            else:
                logger.error(f"Failed to create account entity: {response.status_code} - {response.text}")
                raise RequestException(f"Failed to create account entity: {response.status_code}")
        except RequestException as e:
            logger.error(f"Error creating account entity for {account_data['accountNumber']}: {str(e)}")
            raise

    def create_transaction_entity(self, transaction_data: Dict) -> str:
        """
        Create a financial transaction entity in Atlas.
        
        Args:
            transaction_data (Dict): Transaction data to create
        
        Returns:
            str: GUID of the created entity
        
        Raises:
            ValueError: If required fields are missing or invalid
            RequestException: If the API request fails
        """
        required_fields = ["transactionID", "accountID", "transactionType", "amount", "currency", "transactionDate", "status"]
        for field in required_fields:
            if field not in transaction_data or transaction_data[field] is None:
                raise ValueError(f"Missing or null required field: {field}")
        
        try:
            # Parse transactionDate and convert to milliseconds
            transaction_date = parse_date(transaction_data['transactionDate'])
            transaction_date_ms = int(transaction_date.timestamp() * 1000)  # Convert to milliseconds
        except ValueError as e:
            raise ValueError(f"Invalid transactionDate format: {transaction_data['transactionDate']}")
        
        try:
            qualified_name = f"transaction-{transaction_data['transactionID']}"
            
            entity = {
                "entity": {
                    "typeName": "fin_transaction",
                    "attributes": {
                        "name": f"Transaction {transaction_data['transactionID']}",
                        "qualifiedName": qualified_name,
                        "transactionID": str(transaction_data['transactionID']),
                        "accountID": str(transaction_data['accountID']),
                        "transactionType": str(transaction_data['transactionType']),
                        "amount": float(transaction_data['amount']),
                        "currency": str(transaction_data['currency']),
                        "transactionDate": transaction_date_ms,  # Use milliseconds
                        "merchantName": str(transaction_data.get('merchantName', '')) if transaction_data.get('merchantName') else '',
                        "category": str(transaction_data.get('category', '')) if transaction_data.get('category') else '',
                        "status": str(transaction_data['status'])
                    }
                }
            }
            
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity",
                json=entity,
                headers=self.headers,
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                response_json = response.json()
                guid_assignments = response_json.get("guidAssignments", {})
                guid = guid_assignments.get(qualified_name) or response_json.get("mutatedEntities", {}).get("CREATE", [{}])[0].get("guid")
                if guid:
                    logger.info(f"Created transaction entity with GUID: {guid}")
                    return guid
                else:
                    logger.error(f"No GUID found in response for transaction {qualified_name}")
                    raise RequestException("No GUID assigned for created transaction entity")
            elif response.status_code == 400 or (response.status_code == 404 and "ATLAS-404-00-007" in response.text):
                logger.error(f"Invalid transaction entity data: {response.text}")
                raise RequestException(f"Invalid transaction entity data: {response.text}")
            elif response.status_code == 401:
                logger.error("Authentication failed")
                raise RequestException("Atlas authentication failed")
            elif response.status_code == 409:
                logger.info(f"Transaction entity {qualified_name} already exists")
                search_response = requests.get(
                    f"{self.atlas_url}/api/atlas/v2/search/basic?typeName=fin_transaction&query=qualifiedName:{qualified_name}",
                    headers=self.headers,
                    auth=self.auth
                )
                if search_response.status_code == 200 and search_response.json().get("entities"):
                    return search_response.json()["entities"][0]["guid"]
                raise RequestException(f"Failed to retrieve existing transaction entity GUID")
            else:
                logger.error(f"Failed to create transaction entity: {response.status_code} - {response.text}")
                raise RequestException(f"Failed to create transaction entity: {response.status_code}")
        except RequestException as e:
            logger.error(f"Error creating transaction entity for {transaction_data['transactionID']}: {str(e)}")
            raise

    def classify_entity(self, entity_guid: str, classification_name: str, attributes: Dict = None) -> bool:
        """
        Apply a classification to an entity.
        
        Args:
            entity_guid (str): GUID of the entity to classify
            classification_name (str): Name of the classification to apply
            attributes (Dict, optional): Classification attributes
        
        Returns:
            bool: True if classification was successful, False otherwise
        
        Raises:
            RequestException: If the API request fails
        """
        try:
            classification = {
                "typeName": classification_name
            }
            
            if attributes:
                classification["attributes"] = attributes.copy()
                # Convert date attributes to milliseconds if present
                for key, value in classification["attributes"].items():
                    if "Date" in key and isinstance(value, str):
                        try:
                            parsed_date = parse_date(value)
                            classification["attributes"][key] = int(parsed_date.timestamp() * 1000)
                        except ValueError:
                            logger.warning(f"Invalid date format for {key}: {value}")
                
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/entity/guid/{entity_guid}/classifications",
                json=[classification],
                headers=self.headers,
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Applied classification {classification_name} to entity {entity_guid}")
                return True
            elif response.status_code == 400 or (response.status_code == 404 and "ATLAS-404-00-007" in response.text):
                logger.error(f"Invalid classification data: {response.text}")
                raise RequestException(f"Failed to apply classification: {response.text}")
            else:
                logger.error(f"Failed to apply classification: {response.status_code} - {response.text}")
                raise RequestException(f"Failed to apply classification: {response.status_code}")
        except RequestException as e:
            logger.error(f"Error applying classification to {entity_guid}: {str(e)}")
            raise

    def create_financial_classifications(self) -> bool:
        """
        Create essential financial classifications in Atlas.
        
        Returns:
            bool: True if classifications were created successfully, False otherwise
        """
        classifications = [
            {
                "name": "Customer_Sensitive",
                "description": "Contains customer sensitive information",
                "attributeDefs": [
                    {"name": "dataOwner", "typeName": "string", "isOptional": True},
                    {"name": "reviewDate", "typeName": "date", "isOptional": True}
                ]
            },
            {
                "name": "PII",
                "description": "Personally Identifiable Information",
                "attributeDefs": [
                    {"name": "piiType", "typeName": "string", "isOptional": True},
                    {"name": "encryptionRequired", "typeName": "boolean", "isOptional": True}
                ]
            },
            {
                "name": "High_Value_Transaction",
                "description": "Transaction above a certain value threshold",
                "attributeDefs": [
                    {"name": "thresholdValue", "typeName": "float", "isOptional": True},
                    {"name": "requiresApproval", "typeName": "boolean", "isOptional": True}
                ]
            },
            {
                "name": "Financial_Regulatory",
                "description": "Subject to financial regulations",
                "attributeDefs": [
                    {"name": "regulationType", "typeName": "string", "isOptional": True},
                    {"name": "complianceOwner", "typeName": "string", "isOptional": True}
                ]
            }
        ]
        
        try:
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/types/typedefs",
                json={"classificationDefs": classifications},
                headers=self.headers,
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("Financial classifications created successfully")
                return True
            elif response.status_code == 409:
                logger.info("Financial classifications already exist")
                return True
            else:
                logger.error(f"Failed to create classifications: {response.status_code} - {response.text}")
                return False
        except RequestException as e:
            logger.error(f"Error creating classifications: {str(e)}")
            return False

    def create_financial_entity_types(self) -> bool:
        """
        Create financial entity types in Atlas.
        
        Returns:
            bool: True if entity types were created successfully, False otherwise
        """
        entity_types = [
            {
                "name": "fin_account",
                "description": "Financial Account",
                "attributeDefs": [
                    {"name": "accountNumber", "typeName": "string"},
                    {"name": "accountType", "typeName": "string"},
                    {"name": "balance", "typeName": "float"},
                    {"name": "currency", "typeName": "string"},
                    {"name": "customerID", "typeName": "string"},
                    {"name": "openDate", "typeName": "date"},
                    {"name": "status", "typeName": "string"}
                ]
            },
            {
                "name": "fin_transaction",
                "description": "Financial Transaction",
                "attributeDefs": [
                    {"name": "transactionID", "typeName": "string"},
                    {"name": "accountID", "typeName": "string"},
                    {"name": "transactionType", "typeName": "string"},
                    {"name": "amount", "typeName": "float"},
                    {"name": "currency", "typeName": "string"},
                    {"name": "transactionDate", "typeName": "date"},
                    {"name": "merchantName", "typeName": "string", "isOptional": True},
                    {"name": "category", "typeName": "string", "isOptional": True},
                    {"name": "status", "typeName": "string"}
                ]
            }
        ]
        
        try:
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/types/typedefs",
                json={"entityDefs": entity_types},
                headers=self.headers,
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("Financial entity types created successfully")
                return True
            elif response.status_code == 409:
                logger.info("Financial entity types already exist")
                return True
            else:
                logger.error(f"Failed to create entity types: {response.status_code} - {response.text}")
                return False
        except RequestException as e:
            logger.error(f"Error creating entity types: {str(e)}")
            return False

    def create_business_glossary(self) -> Optional[str]:
        """
        Create a business glossary for financial terms.
        
        Returns:
            str: GUID of the created glossary or None if failed
        """
        glossary = {
            "name": "Financial_Glossary",
            "displayName": "Financial Glossary",
            "shortDescription": "Business glossary for financial terms",
            "longDescription": "Detailed description of financial terms used in the organization"
        }
        
        try:
            # Check if glossary already exists
            response = requests.get(
                f"{self.atlas_url}/api/atlas/v2/glossary",
                params={"name": "Financial_Glossary"},
                headers=self.headers,
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                glossaries = response.json()
                if glossaries:
                    return glossaries[0]["guid"]
            
            # Create new glossary if it doesn't exist
            response = requests.post(
                f"{self.atlas_url}/api/atlas/v2/glossary",
                json=glossary,
                headers=self.headers,
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                glossary_info = response.json()
                logger.info(f"Created business glossary: {glossary_info['guid']}")
                return glossary_info["guid"]
            else:
                logger.error(f"Failed to create glossary: {response.status_code} - {response.text}")
                return None
        except RequestException as e:
            logger.error(f"Error creating business glossary: {str(e)}")
            return None