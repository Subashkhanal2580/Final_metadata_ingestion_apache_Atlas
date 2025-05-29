#!/usr/bin/env python
"""
Create Financial Business Glossary for Apache Atlas

This script creates a comprehensive financial business glossary in Apache Atlas
to provide standardized terminology for financial data assets.
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

class AtlasGlossaryManager:
    def __init__(self, atlas_url, username="admin", password="admin"):
        """Initialize Atlas client with connection details."""
        self.atlas_url = atlas_url.rstrip('/')
        self.auth = (username, password)
        self.headers = {"Content-Type": "application/json", "Accept": "application/json"}
        logger.info(f"Initializing Atlas glossary manager for {atlas_url}")
    
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
    
    def create_financial_glossary(self):
        """Create a financial business glossary in Atlas."""
        try:
            # Create the glossary
            glossary = {
                "name": "Financial_Glossary",
                "shortDescription": "Financial terms and definitions",
                "longDescription": "Comprehensive glossary of financial terms used in transaction processing and financial data management",
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
                return glossary_guid
            else:
                logger.error(f"Failed to create glossary: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error creating glossary: {e}")
            return None
    
    def create_glossary_terms(self, glossary_guid):
        """Create financial glossary terms."""
        if not glossary_guid:
            logger.error("No glossary GUID provided")
            return False
        
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
            },
            {
                "name": "Transaction_Amount",
                "shortDescription": "Value of transaction",
                "longDescription": "The monetary value of a financial transaction",
                "examples": ["$50.00", "€100.50"],
                "anchor": {"glossaryGuid": glossary_guid}
            },
            {
                "name": "Account_Balance",
                "shortDescription": "Current account value",
                "longDescription": "The current monetary value held in a financial account",
                "examples": ["$1,250.75", "€5,000.00"],
                "anchor": {"glossaryGuid": glossary_guid}
            },
            {
                "name": "Transaction_Type",
                "shortDescription": "Category of transaction",
                "longDescription": "The classification of a financial transaction based on its nature",
                "examples": ["Deposit", "Withdrawal", "Transfer", "Payment"],
                "anchor": {"glossaryGuid": glossary_guid}
            },
            {
                "name": "Currency",
                "shortDescription": "Monetary unit",
                "longDescription": "The medium of exchange used in financial transactions",
                "examples": ["USD", "EUR", "JPY", "GBP"],
                "anchor": {"glossaryGuid": glossary_guid}
            },
            {
                "name": "Transaction_Date",
                "shortDescription": "Date of transaction",
                "longDescription": "The date and time when a financial transaction occurred",
                "examples": ["2023-05-28 14:30:00", "2023-06-01 09:15:22"],
                "anchor": {"glossaryGuid": glossary_guid}
            },
            {
                "name": "Transaction_Status",
                "shortDescription": "Current state of transaction",
                "longDescription": "The current processing status of a financial transaction",
                "examples": ["Completed", "Pending", "Failed", "Reversed"],
                "anchor": {"glossaryGuid": glossary_guid}
            },
            {
                "name": "High_Value_Transaction",
                "shortDescription": "Large monetary transaction",
                "longDescription": "A financial transaction that exceeds a specified threshold value",
                "examples": ["$10,000 wire transfer", "€50,000 property down payment"],
                "anchor": {"glossaryGuid": glossary_guid}
            }
        ]
        
        success_count = 0
        for term in terms:
            try:
                term_response = requests.post(
                    f"{self.atlas_url}/api/atlas/v2/glossary/term",
                    json=term,
                    headers=self.headers,
                    auth=self.auth
                )
                
                if term_response.status_code == 200:
                    logger.info(f"Created term: {term['name']}")
                    success_count += 1
                else:
                    logger.warning(f"Failed to create term {term['name']}: {term_response.status_code} - {term_response.text}")
            except Exception as e:
                logger.error(f"Error creating term {term['name']}: {e}")
        
        logger.info(f"Successfully created {success_count} out of {len(terms)} terms")
        return success_count == len(terms)
    
    def create_term_relationships(self, glossary_guid):
        """Create relationships between glossary terms."""
        if not glossary_guid:
            logger.error("No glossary GUID provided")
            return False
        
        # First, get all terms in the glossary
        try:
            response = requests.get(
                f"{self.atlas_url}/api/atlas/v2/glossary/{glossary_guid}/terms",
                headers=self.headers,
                auth=self.auth
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get glossary terms: {response.status_code} - {response.text}")
                return False
            
            terms = response.json()
            term_map = {term["name"]: term["guid"] for term in terms}
            
            # Define relationships
            relationships = [
                {
                    "related_term1": "Transaction",
                    "related_term2": "Transaction_Amount",
                    "relation_type": "hasA"
                },
                {
                    "related_term1": "Transaction",
                    "related_term2": "Transaction_Type",
                    "relation_type": "hasA"
                },
                {
                    "related_term1": "Transaction",
                    "related_term2": "Transaction_Date",
                    "relation_type": "hasA"
                },
                {
                    "related_term1": "Transaction",
                    "related_term2": "Transaction_Status",
                    "relation_type": "hasA"
                },
                {
                    "related_term1": "Account",
                    "related_term2": "Account_Balance",
                    "relation_type": "hasA"
                },
                {
                    "related_term1": "High_Value_Transaction",
                    "related_term2": "Transaction",
                    "relation_type": "isA"
                }
            ]
            
            success_count = 0
            for rel in relationships:
                if rel["related_term1"] in term_map and rel["related_term2"] in term_map:
                    try:
                        relation_data = {
                            "relation": {
                                "typeName": rel["relation_type"],
                                "end1": {
                                    "guid": term_map[rel["related_term1"]]
                                },
                                "end2": {
                                    "guid": term_map[rel["related_term2"]]
                                }
                            }
                        }
                        
                        relation_response = requests.post(
                            f"{self.atlas_url}/api/atlas/v2/relationship",
                            json=relation_data,
                            headers=self.headers,
                            auth=self.auth
                        )
                        
                        if relation_response.status_code == 200:
                            logger.info(f"Created relationship: {rel['related_term1']} {rel['relation_type']} {rel['related_term2']}")
                            success_count += 1
                        else:
                            logger.warning(f"Failed to create relationship: {relation_response.status_code} - {relation_response.text}")
                    except Exception as e:
                        logger.error(f"Error creating relationship: {e}")
                else:
                    logger.warning(f"Terms not found for relationship: {rel}")
            
            logger.info(f"Successfully created {success_count} out of {len(relationships)} relationships")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error creating term relationships: {e}")
            return False

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Create financial business glossary in Apache Atlas')
    parser.add_argument('--atlas-url', default='http://localhost:21000', help='Atlas server URL')
    parser.add_argument('--username', default='admin', help='Atlas username')
    parser.add_argument('--password', default='admin', help='Atlas password')
    
    args = parser.parse_args()
    
    # Initialize the Atlas client
    atlas_client = AtlasGlossaryManager(args.atlas_url, args.username, args.password)
    
    # Check connection
    if not atlas_client.check_connection():
        logger.error("Failed to connect to Atlas. Exiting.")
        sys.exit(1)
    
    # Create financial glossary
    glossary_guid = atlas_client.create_financial_glossary()
    if not glossary_guid:
        logger.error("Failed to create financial glossary. Exiting.")
        sys.exit(1)
    
    # Create glossary terms
    if not atlas_client.create_glossary_terms(glossary_guid):
        logger.warning("Some terms could not be created")
    
    # Create term relationships
    if not atlas_client.create_term_relationships(glossary_guid):
        logger.warning("Some term relationships could not be created")
    
    logger.info("Script completed successfully")

if __name__ == "__main__":
    main()
