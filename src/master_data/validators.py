"""Pagination/sort validation (ported from Grant rcs_engine.utils.validators)."""
from typing import Optional, Tuple

from src.master_data.exceptions import ValidationError


def validate_pagination_params(page: int, page_size: int) -> Tuple[int, int]:
    if page < 1:
        raise ValidationError("Page number must be greater than 0")
    if page_size < 1 or page_size > 1000:
        raise ValidationError("Page size must be between 1 and 1000")
    return page, page_size


def validate_sort_params(sort_by: Optional[str], sort_dir: Optional[str]) -> Tuple[Optional[str], str]:
    if sort_dir and sort_dir.lower() not in ["asc", "desc"]:
        raise ValidationError("Sort direction must be 'asc' or 'desc'")
    return sort_by, (sort_dir or "asc").lower()
