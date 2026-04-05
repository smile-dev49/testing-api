"""
Minimal helpers for view APIs (no Parser config / tab_mapping dependency).
"""
import re
from typing import Optional


def extract_number_from_deposit_number(deposit_number: str) -> Optional[int]:
    """
    Extract number from deposit_number pattern like L{number} or 1_L{number}.
    Examples: L170126811 -> 170126811, 1_L170126811 -> 170126811
    """
    if not deposit_number:
        return None
    match = re.search(r"L(\d+)", deposit_number)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None
