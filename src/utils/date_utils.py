"""
Date utility functions for consistent date handling across the application.

Rules:
1. Dates are saved to the database as datetime objects (proper date type)
2. Dates are displayed in API responses as dd/mm/yyyy format
3. All dates must have year, month, and day components
4. Supports parsing from multiple input formats
"""
import logging
from datetime import datetime, date
from typing import Optional, Union, Any

logger = logging.getLogger(__name__)
INPUT_DATE_FORMATS = [
    '%d/%m/%Y',     
    '%Y-%m-%d',     
    '%d-%m-%Y',     
    '%Y/%m/%d',     
    '%d.%m.%Y',     
]

OUTPUT_DATE_FORMAT = '%d/%m/%Y'  


def parse_date(date_input: Optional[Union[str, datetime, date]]) -> Optional[datetime]:
    """
    Parse date from various formats to datetime object.
    
    Args:
        date_input: Date as string, datetime, or date object
        
    Returns:
        datetime object with year, month, and day, or None if invalid
        
    Examples:
        >>> parse_date("24/02/2020")
        datetime.datetime(2020, 2, 24, 0, 0)
        >>> parse_date("2020-02-24")
        datetime.datetime(2020, 2, 24, 0, 0)
        >>> parse_date(datetime(2020, 2, 24))
        datetime.datetime(2020, 2, 24, 0, 0)
    """
    if not date_input:
        return None
    if isinstance(date_input, datetime):
        return date_input
    if isinstance(date_input, date):
        return datetime.combine(date_input, datetime.min.time())
    if not isinstance(date_input, str):
        return None
    
    date_str = str(date_input).strip()
    if not any(ch.isdigit() for ch in date_str):
        return None
    for fmt in INPUT_DATE_FORMATS:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            if parsed_date.year and parsed_date.month and parsed_date.day:
                return parsed_date
        except (ValueError, TypeError):
            continue
    
    logger.warning(f"Could not parse date string: {date_str}")
    return None


def format_date_for_display(date_input: Optional[Union[str, datetime, date]]) -> Optional[str]:
    """
    Format date for display as dd/mm/yyyy.
    
    Args:
        date_input: Date as string, datetime, or date object
        
    Returns:
        Date formatted as dd/mm/yyyy string, or None if invalid
        
    Examples:
        >>> format_date_for_display(datetime(2020, 2, 24))
        "24/02/2020"
        >>> format_date_for_display("2020-02-24")
        "24/02/2020"
        >>> format_date_for_display("24/02/2020")
        "24/02/2020"
    """
    if not date_input:
        return None
    
    if isinstance(date_input, str):
        date_str = date_input.strip()
        try:
            parsed = datetime.strptime(date_str, OUTPUT_DATE_FORMAT)
            return date_str
        except (ValueError, TypeError):
            pass
    
    parsed_date = parse_date(date_input)
    
    if not parsed_date:
        return None
    
    try:
        return parsed_date.strftime(OUTPUT_DATE_FORMAT)
    except (ValueError, AttributeError) as e:
        logger.warning(f"Could not format date {parsed_date}: {e}")
        return None


def format_date_for_storage(date_input: Optional[Union[str, datetime, date]]) -> Optional[datetime]:
    """
    Format date for storage in MongoDB as datetime object.
    
    Args:
        date_input: Date as string, datetime, or date object
        
    Returns:
        datetime object ready for MongoDB storage, or None if invalid
        
    Note:
        MongoDB stores dates as ISODate (UTC datetime), but we store without timezone info
        to avoid complications. The date represents a calendar date (year/month/day).
    """
    return parse_date(date_input)


def normalize_date_string(date_str: Optional[Any]) -> Optional[str]:
    """
    Normalize date-like strings for processing.
    Used during data transformation to filter out placeholder dates.
    
    Args:
        date_str: Date string or datetime object
        
    Returns:
        Normalized date string in dd/mm/yyyy format, or None if invalid/placeholder
        
    Examples:
        >>> normalize_date_string("AAAA")
        None
        >>> normalize_date_string("JJ/MM/AAAA")
        None
        >>> normalize_date_string("24/02/2020")
        "24/02/2020"
    """
    if not date_str:
        return None
    if isinstance(date_str, datetime):
        return format_date_for_display(date_str)
    s = str(date_str).strip()
    if not any(ch.isdigit() for ch in s):
        return None
    parsed = parse_date(s)
    if parsed:
        return format_date_for_display(parsed)
    return s


def serialize_date_for_api(date_obj: Optional[Union[str, datetime, date]]) -> Optional[str]:
    """
    Serialize date for API response as dd/mm/yyyy.
    
    This is the main function to use when preparing dates for API responses.
    Ensures all dates are displayed consistently as dd/mm/yyyy.
    
    Args:
        date_obj: Date as string, datetime, or date object
        
    Returns:
        Date formatted as dd/mm/yyyy string, or None if invalid
    """
    return format_date_for_display(date_obj)
DATE_YEAR_PLACEHOLDERS = frozenset({"AAAA", "YYYY", "JJ", "MM", "JJ/MM/AAAA", "DD/MM/YYYY", "DD-MM-YYYY"})


def is_placeholder_date_or_year(value: Optional[Any]) -> bool:
    """
    Return True if value is a placeholder (e.g. AAAA, YYYY) and should be stored as null.
    """
    if value is None:
        return True
    s = str(value).strip().upper()
    if not s:
        return True
    if s in DATE_YEAR_PLACEHOLDERS:
        return True
    if not any(ch.isdigit() for ch in s):
        return True
    return False


def normalize_year_value(value: Optional[Any]) -> Optional[str]:
    """
    Normalize a year value for DB storage. Placeholders (AAAA, YYYY, etc.) become None.
    Returns 4-digit year string or None.
    """
    if value is None or is_placeholder_date_or_year(value):
        return None
    s = str(value).strip()
    if len(s) == 4 and s.isdigit():
        return s
    if len(s) >= 4 and s[-4:].isdigit():
        return s[-4:]
    parsed = parse_date(value)
    if parsed and parsed.year:
        return str(parsed.year)
    return None


def extract_year_from_date(date_input: Optional[Union[str, datetime, date]]) -> Optional[str]:
    """
    Extract year from date as string.
    
    Args:
        date_input: Date as string, datetime, or date object
        
    Returns:
        Year as 4-digit string, or None if invalid
        
    Examples:
        >>> extract_year_from_date("24/02/2020")
        "2020"
        >>> extract_year_from_date(datetime(2020, 2, 24))
        "2020"
    """
    parsed = parse_date(date_input)
    if parsed and parsed.year:
        return str(parsed.year)
    if isinstance(date_input, str) and len(date_input) >= 4:
        date_str = date_input.strip()
        try:
            dt = datetime.strptime(date_str, '%d/%m/%Y')
            return str(dt.year)
        except:
            if date_str[-4:].isdigit():
                return date_str[-4:]
    
    return None
parse_date_string = parse_date
