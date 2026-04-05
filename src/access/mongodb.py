"""
MongoDB connection management
"""
import os
import logging
from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)

_mongodb_client: Optional[MongoClient] = None
_database: Optional[Database] = None


def get_mongodb_client() -> MongoClient:
    """
    Get or create MongoDB client connection.
    Uses singleton pattern to reuse connection.
    
    Returns:
        MongoClient: MongoDB client instance
        
    Raises:
        ConnectionFailure: If unable to connect to MongoDB
    """
    global _mongodb_client
    
    if _mongodb_client is None:
        mongodb_url = os.getenv(
            "MONGODB_URL",
            "mongodb://localhost:27017/rcs_parser"  # Default: no auth for local development
        )
        if not mongodb_url or not mongodb_url.strip():
            mongodb_url = "mongodb://localhost:27017/rcs_parser"
        
        try:
            log_url = mongodb_url
            if '@' in mongodb_url:
                parts = mongodb_url.split('@')
                log_url = f"mongodb://***@{parts[1]}" if len(parts) > 1 else mongodb_url
            
            logger.info(f"Connecting to MongoDB: {log_url}")
            _mongodb_client = MongoClient(
                mongodb_url,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            try:
                _mongodb_client.admin.command('ping')
            except Exception as auth_error:
                if 'Authentication failed' in str(auth_error) and '@' in mongodb_url:
                    logger.info("Authentication failed, trying without auth...")
                    db_name = mongodb_url.split('/')[-1].split('?')[0] if '/' in mongodb_url else "rcs_parser"
                    simple_url = f"mongodb://localhost:27017/{db_name}"
                    _mongodb_client.close()
                    _mongodb_client = MongoClient(
                        simple_url,
                        serverSelectionTimeoutMS=5000,
                        connectTimeoutMS=5000,
                        socketTimeoutMS=5000
                    )
                    _mongodb_client.admin.command('ping')
                else:
                    raise
            
            logger.info("PASS: MongoDB connection established successfully")
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise ConnectionFailure(f"Unable to connect to MongoDB: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting to MongoDB: {e}")
            raise
    
    return _mongodb_client


def get_database(database_name: Optional[str] = None) -> Database:
    """
    Get MongoDB database instance.
    
    Args:
        database_name: Name of the database. If None, extracts from MONGODB_URL
        
    Returns:
        Database: MongoDB database instance
    """
    global _database
    
    if _database is None:
        client = get_mongodb_client()
        
        if database_name is None:
            mongodb_url = os.getenv(
                "MONGODB_URL",
                "mongodb://admin:admin123@localhost:27017/rcs_parser?authSource=admin"
            )
            if '/' in mongodb_url:
                db_part = mongodb_url.split('/')[-1].split('?')[0]
                database_name = db_part if db_part else "rcs_parser"
            else:
                database_name = "rcs_parser"
        
        _database = client[database_name]
        logger.info(f"Using MongoDB database: {database_name}")
    
    return _database


def database_exists(database_name: Optional[str] = None) -> bool:
    """
    Check if a MongoDB database exists.
    
    Args:
        database_name: Name of the database. If None, extracts from MONGODB_URL
        
    Returns:
        bool: True if database exists, False otherwise
    """
    try:
        client = get_mongodb_client()
        
        if database_name is None:
            mongodb_url = os.getenv(
                "MONGODB_URL",
                "mongodb://admin:admin123@localhost:27017/rcs_parser?authSource=admin"
            )
            if '/' in mongodb_url:
                db_part = mongodb_url.split('/')[-1].split('?')[0]
                database_name = db_part if db_part else "rcs_parser"
            else:
                database_name = "rcs_parser"
        
        # List all databases and check if the target database exists
        db_list = client.list_database_names()
        exists = database_name in db_list
        
        if exists:
            logger.info(f"Database '{database_name}' exists")
        else:
            logger.info(f"Database '{database_name}' does not exist")
        
        return exists
    except Exception as e:
        logger.warning(f"Could not check if database exists: {e}")
        return False


def close_mongodb_connection():
    """
    Close MongoDB connection.
    Should be called on application shutdown.
    """
    global _mongodb_client, _database
    
    if _mongodb_client:
        try:
            _mongodb_client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")
        finally:
            _mongodb_client = None
            _database = None
