"""
Utility functions for metadata ingestion.
"""

import logging
from typing import Dict, Any, List
import json
from datetime import datetime

logger = logging.getLogger(__name__)

def format_entity_data(entity_type: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format entity data according to Atlas API requirements.
    
    Args:
        entity_type: Type of the entity (e.g., 'database', 'table')
        attributes: Dictionary of entity attributes
        
    Returns:
        Formatted entity data dictionary
    """
    return {
        "typeName": entity_type,
        "attributes": attributes,
        "createdBy": "metadata_ingestion",
        "createTime": datetime.utcnow().isoformat(),
        "version": 1
    }

def validate_entity_data(entity_data: Dict[str, Any]) -> bool:
    """
    Validate entity data structure.
    
    Args:
        entity_data: Entity data dictionary to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ["typeName", "attributes"]
    
    try:
        # Check required fields
        for field in required_fields:
            if field not in entity_data:
                logger.error(f"Missing required field: {field}")
                return False
        
        # Validate attributes
        if not isinstance(entity_data["attributes"], dict):
            logger.error("Attributes must be a dictionary")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error validating entity data: {str(e)}")
        return False

def format_error_response(error: Exception) -> Dict[str, Any]:
    """
    Format error response for API calls.
    
    Args:
        error: Exception object
        
    Returns:
        Formatted error response dictionary
    """
    return {
        "error": {
            "message": str(error),
            "type": error.__class__.__name__,
            "timestamp": datetime.utcnow().isoformat()
        }
    }

def batch_entities(entities: List[Dict[str, Any]], batch_size: int) -> List[List[Dict[str, Any]]]:
    """
    Split entities into batches for batch processing.
    
    Args:
        entities: List of entity dictionaries
        batch_size: Size of each batch
        
    Returns:
        List of entity batches
    """
    return [entities[i:i + batch_size] for i in range(0, len(entities), batch_size)]

def log_entity_operation(operation: str, entity_type: str, entity_id: str, status: str):
    """
    Log entity operation details.
    
    Args:
        operation: Operation type (create, update, delete)
        entity_type: Type of entity
        entity_id: Entity identifier
        status: Operation status
    """
    logger.info(
        f"Entity operation: {operation} | "
        f"Type: {entity_type} | "
        f"ID: {entity_id} | "
        f"Status: {status}"
    ) 