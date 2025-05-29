"""
Metadata models for Apache Atlas entities.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Database:
    """Database entity model."""
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    location: Optional[str] = None
    parameters: Optional[Dict[str, str]] = None
    
    def to_atlas_entity(self) -> Dict[str, Any]:
        """Convert to Atlas entity format."""
        return {
            "typeName": "database",
            "attributes": {
                "name": self.name,
                "description": self.description,
                "owner": self.owner,
                "location": self.location,
                "parameters": self.parameters or {}
            }
        }

@dataclass
class Table:
    """Table entity model."""
    name: str
    database: str
    description: Optional[str] = None
    owner: Optional[str] = None
    table_type: Optional[str] = None
    create_time: Optional[datetime] = None
    parameters: Optional[Dict[str, str]] = None
    
    def to_atlas_entity(self) -> Dict[str, Any]:
        """Convert to Atlas entity format."""
        return {
            "typeName": "table",
            "attributes": {
                "name": self.name,
                "database": self.database,
                "description": self.description,
                "owner": self.owner,
                "tableType": self.table_type,
                "createTime": self.create_time.isoformat() if self.create_time else None,
                "parameters": self.parameters or {}
            }
        }

@dataclass
class Column:
    """Column entity model."""
    name: str
    table: str
    database: str
    data_type: str
    description: Optional[str] = None
    position: Optional[int] = None
    is_partition: bool = False
    
    def to_atlas_entity(self) -> Dict[str, Any]:
        """Convert to Atlas entity format."""
        return {
            "typeName": "column",
            "attributes": {
                "name": self.name,
                "table": self.table,
                "database": self.database,
                "dataType": self.data_type,
                "description": self.description,
                "position": self.position,
                "isPartition": self.is_partition
            }
        }

@dataclass
class Process:
    """Process entity model."""
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    inputs: Optional[List[str]] = None
    outputs: Optional[List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def to_atlas_entity(self) -> Dict[str, Any]:
        """Convert to Atlas entity format."""
        return {
            "typeName": "process",
            "attributes": {
                "name": self.name,
                "description": self.description,
                "owner": self.owner,
                "inputs": self.inputs or [],
                "outputs": self.outputs or [],
                "startTime": self.start_time.isoformat() if self.start_time else None,
                "endTime": self.end_time.isoformat() if self.end_time else None
            }
        } 