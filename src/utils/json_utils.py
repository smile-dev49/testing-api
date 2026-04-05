"""
JSON serialization utilities for API responses.
Ensures consistent date formatting (dd/mm/yyyy) across all API responses.
"""
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Union
from bson import ObjectId

from .date_utils import serialize_date_for_api

logger = logging.getLogger(__name__)


def serialize_for_api(data: Any) -> Any:
    """
    Recursively serialize data for API response.
    
    - Converts datetime and date objects to dd/mm/yyyy format
    - Converts ObjectId to string
    - Handles nested dictionaries, lists, and other structures
    
    Args:
        data: Data to serialize (dict, list, or any JSON-serializable type)
        
    Returns:
        Serialized data ready for JSON response
    """
    if data is None:
        return None
    
    if isinstance(data, (datetime, date)):
        return serialize_date_for_api(data)
    
    if isinstance(data, ObjectId):
        return str(data)
    
    if isinstance(data, dict):
        return {key: serialize_for_api(value) for key, value in data.items()}
    
    if isinstance(data, (list, tuple)):
        return [serialize_for_api(item) for item in data]
    
    return data


def prepare_document_for_api(document: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare MongoDB document for API response.
    
    - Converts _id to string
    - Converts all dates to dd/mm/yyyy format
    - Handles nested structures
    
    Args:
        document: MongoDB document dictionary
        
    Returns:
        Document ready for API response
    """
    if not document:
        return document
    
    result = dict(document)
    
    if "_id" in result:
        result["_id"] = str(result["_id"])
    
    return serialize_for_api(result)


def prepare_documents_for_api(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare multiple MongoDB documents for API response.
    
    Args:
        documents: List of MongoDB document dictionaries
        
    Returns:
        List of documents ready for API response
    """
    return [prepare_document_for_api(doc) for doc in documents]


class DateAwareJSONEncoder:
    """
    Custom JSON encoder that formats dates as dd/mm/yyyy.
    Can be used with FastAPI response_model or json.dumps.
    """
    
    @staticmethod
    def default(obj: Any) -> Any:
        """Convert dates and other non-serializable objects."""
        if isinstance(obj, (datetime, date)):
            return serialize_date_for_api(obj)
        if isinstance(obj, ObjectId):
            return str(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
