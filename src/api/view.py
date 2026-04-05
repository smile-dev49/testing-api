"""
API endpoints for parsing results view (JSON only; no HTML/JS templates).
"""
import logging

from fastapi import APIRouter, HTTPException, status
from pymongo.errors import OperationFailure

from ..access.mongodb import get_database
from ..utils.json_utils import serialize_for_api

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/view", tags=["View"])


@router.get("/status/rcs-numbers", summary="Get all RCS numbers from filings collection")
async def get_status_rcs_numbers():
    """
    Get all RCS numbers that have filings data.

    Returns a list of RCS numbers from the filings collection.
    """
    try:
        db = get_database()
        filings_collection = db["filings"]

        rcs_numbers = filings_collection.distinct("rcs_number")

        logger.info(f"Found {len(rcs_numbers)} distinct RCS numbers in filings collection")

        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            try:
                filing_doc = filings_collection.find_one({"rcs_number": rcs_number})
                if filing_doc:
                    filings_dict = filing_doc.get("filings", {})
                    if not isinstance(filings_dict, dict):
                        filings_dict = {}
                    total_deposits = len(filings_dict)
                    status_counts = {"success": 0, "failed": 0, "skipped": 0, "pending": 0}
                    for deposit_number, filing_info in filings_dict.items():
                        filing_status = filing_info.get("status", "pending")
                        if filing_status in status_counts:
                            status_counts[filing_status] += 1
                        else:
                            status_counts["pending"] += 1

                    rcs_list.append(
                        {
                            "rcs_number": rcs_number,
                            "total_deposits": total_deposits,
                            "status_counts": status_counts,
                            "job_id": filing_doc.get("job_id"),
                            "created_at": serialize_for_api(filing_doc.get("created_at")),
                            "updated_at": serialize_for_api(filing_doc.get("updated_at")),
                        }
                    )
            except Exception as e:
                logger.warning(f"Error processing RCS {rcs_number}: {e}")
                rcs_list.append(
                    {
                        "rcs_number": rcs_number,
                        "total_deposits": 0,
                        "status_counts": {"success": 0, "failed": 0, "skipped": 0, "pending": 0},
                        "job_id": None,
                        "created_at": None,
                        "updated_at": None,
                    }
                )

        logger.info(f"Returning {len(rcs_list)} RCS numbers")

        return {"total": len(rcs_list), "rcs_numbers": rcs_list}

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving status RCS numbers: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve status RCS numbers: {str(e)}",
        )


@router.get("/status/{rcs_number}/deposits", summary="Get deposit numbers with status and metadata for an RCS number")
async def get_status_deposits(rcs_number: str):
    """
    Get all deposit numbers with their status and metadata for a given RCS number.

    Returns deposit numbers with only status and metadata fields.
    """
    try:
        db = get_database()
        filings_collection = db["filings"]

        rcs_doc = filings_collection.find_one({"rcs_number": rcs_number})

        if not rcs_doc:
            logger.info(f"No filings document found for RCS {rcs_number}")
            return {"rcs_number": rcs_number, "total": 0, "deposits": []}

        filings_dict = rcs_doc.get("filings", {})

        deposits = []
        for deposit_number, filing_info in filings_dict.items():
            deposits.append(
                {
                    "deposit_number": deposit_number,
                    "status": filing_info.get("status", "pending"),
                    "metadata": filing_info.get("metadata", {}),
                    "filing_id": filing_info.get("filing_id"),
                    "filing_date": serialize_for_api(filing_info.get("filing_date")),
                    "corrected_deposit_number": filing_info.get("corrected_deposit_number"),
                    "corrective_deposit_number": filing_info.get("corrective_deposit_number"),
                    "created_at": serialize_for_api(filing_info.get("created_at")),
                    "updated_at": serialize_for_api(filing_info.get("updated_at")),
                }
            )
        deposits.sort(key=lambda x: x.get("deposit_number", ""))

        logger.info(f"Retrieved {len(deposits)} deposit records for RCS {rcs_number}")

        return {"rcs_number": rcs_number, "total": len(deposits), "deposits": deposits}

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving status deposits for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve status deposits: {str(e)}",
        )
