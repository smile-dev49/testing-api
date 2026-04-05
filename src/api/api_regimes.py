"""
API endpoints for Regimes data (Manager Signatory Regime, Daily Management Signatory Regime).
API parity with entity/SMO: rcs-numbers, search, deposits/search, all/regimes.
"""
import logging
import math
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from pymongo.errors import OperationFailure

from ..access.mongodb import get_database
from ..utils.json_utils import serialize_for_api

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/view", tags=["Regimes"])

_REGIME_RECORD_FIELDS = ("type", "details", "registration_id", "last_scan")
_REGIME_Q_SEARCH_FIELDS = ("type", "details")


@router.get("/regime", summary="Get all regime data from all RCS numbers")
async def get_all_regime_data():
    """
    Get all regime data from all RCS numbers.

    Returns all regime records (TA_011_B, TA_012_B) from all RCS numbers.
    Each regime includes rcs_number and deposit_number for display.
    """
    try:
        db = get_database()
        regimes_collection = db["regimes"]

        all_docs = regimes_collection.find({})
        all_regimes_list = []

        for regime_doc in all_docs:
            rcs_number = regime_doc.get("registration_id")
            if not rcs_number:
                continue

            doc_created_at = serialize_for_api(regime_doc.get("created_at"))
            data = regime_doc.get("data", {})

            if not isinstance(data, dict):
                continue

            for deposit_number, regime_records in data.items():
                if not isinstance(regime_records, list):
                    continue

                for regime_record in regime_records:
                    if not isinstance(regime_record, dict):
                        continue

                    regime_copy = dict(regime_record)
                    regime_copy.pop("id", None)
                    regime_copy.pop("status", None)
                    regime_copy["deposit_number"] = deposit_number
                    regime_copy["rcs_number"] = rcs_number
                    regime_copy["legal_name"] = regime_doc.get("legal_name")
                    regime_copy["entity_status"] = regime_doc.get("entity_status")

                    if doc_created_at and ("created_at" not in regime_copy or not regime_copy.get("created_at")):
                        regime_copy["created_at"] = doc_created_at

                    regime_copy = serialize_for_api(regime_copy)
                    all_regimes_list.append(regime_copy)

        def extract_deposit_num(val):
            if not val:
                return 999999999
            match = re.search(r"L(\d+)", str(val))
            if match:
                return int(match.group(1))
            num_match = re.search(r"(\d+)", str(val))
            return int(num_match.group(1)) if num_match else 999999999

        all_regimes_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                extract_deposit_num(x.get("deposit_number", "")),
                x.get("type", ""),
            ),
            reverse=False,
        )

        logger.info(f"Retrieved {len(all_regimes_list)} total regime records from all RCS numbers")

        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": "success",
            "last_scan": None,
            "regimes": all_regimes_list,
            "total": len(all_regimes_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving all regime data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve regime data: {str(e)}",
        )


def _build_regime_search_query(params: dict, q: Optional[str] = None) -> list:
    """Build MongoDB aggregation stages for regime search."""
    root_match = {}
    for field in ("registration_id", "legal_name", "entity_status"):
        val = params.get(field)
        if val is not None and str(val).strip():
            root_match[field] = {"$regex": re.escape(str(val).strip()), "$options": "i"}
    stages = []
    if root_match:
        stages.append({"$match": root_match})
    stages.append({"$addFields": {"dataArr": {"$objectToArray": {"$ifNull": ["$data", {}]}}}})
    stages.append({"$unwind": {"path": "$dataArr", "preserveNullAndEmptyArrays": False}})
    stages.append({"$unwind": {"path": "$dataArr.v", "preserveNullAndEmptyArrays": False}})
    record_match = {}
    v_prefix = "dataArr.v."
    for field in _REGIME_RECORD_FIELDS:
        val = params.get(field)
        if val is not None and str(val).strip():
            record_match[v_prefix + field] = {"$regex": re.escape(str(val).strip()), "$options": "i"}
    if q and q.strip():
        q_esc = re.escape(q.strip())
        regex = {"$regex": q_esc, "$options": "i"}
        record_match["$or"] = [{v_prefix + f: regex} for f in _REGIME_Q_SEARCH_FIELDS]
    if record_match:
        stages.append({"$match": record_match})
    return stages


@router.get("/regimes/rcs-numbers", summary="Get all RCS numbers with regime data")
async def get_regimes_rcs_numbers():
    """Get all RCS numbers that have regime data."""
    try:
        db = get_database()
        regimes_collection = db["regimes"]
        rcs_numbers = regimes_collection.distinct("registration_id")
        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            try:
                doc = regimes_collection.find_one({"registration_id": rcs_number})
                if doc:
                    data = doc.get("data", {}) or {}
                    total_records = sum(len(r) for r in data.values() if isinstance(r, list))
                    rcs_list.append({
                        "rcs_number": rcs_number,
                        "legal_name": doc.get("legal_name"),
                        "status": doc.get("status"),
                        "entity_status": doc.get("entity_status"),
                        "last_scan": serialize_for_api(doc.get("last_scan")),
                        "total_records": total_records,
                    })
                else:
                    rcs_list.append({"rcs_number": rcs_number, "legal_name": None, "status": None, "entity_status": None, "last_scan": None, "total_records": 0})
            except Exception as e:
                logger.warning(f"Error processing RCS {rcs_number}: {e}")
                rcs_list.append({"rcs_number": rcs_number, "legal_name": None, "status": None, "entity_status": None, "last_scan": None, "total_records": 0})
        return {"total": len(rcs_list), "rcs_numbers": rcs_list}
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error retrieving regime RCS numbers: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve regime RCS numbers: {str(e)}")


@router.get("/regimes/search", summary="Search regimes with pagination")
@router.get("/regimes/deposits/search", summary="Search all regime records with pagination (entity-parity)")
async def search_regimes(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    entity_status: Optional[str] = Query(None, description="Filter by entity status"),
    regime_type: Optional[str] = Query(None, description="Filter by regime type (e.g. Manager Signatory Regime)"),
    details: Optional[str] = Query(None, description="Filter by regime details"),
    q: Optional[str] = Query(None, description="Full-text search across type and details"),
):
    """Search regimes by filters with pagination."""
    try:
        params = {
            "registration_id": registration_id, "legal_name": legal_name, "entity_status": entity_status,
            "type": regime_type, "details": details,
        }
        db = get_database()
        regimes_collection = db["regimes"]
        base_pipeline = _build_regime_search_query(params, q)
        skip = (page - 1) * page_size
        facet_pipeline = base_pipeline + [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "items": [
                        {"$sort": {"registration_id": 1, "dataArr.k": 1, "dataArr.v.type": 1}},
                        {"$skip": skip},
                        {"$limit": page_size},
                        {"$project": {"_id": 0, "registration_id": 1, "deposit_number": "$dataArr.k", "record": "$dataArr.v",
                                      "doc_legal_name": "$legal_name", "doc_entity_status": "$entity_status",
                                      "doc_status": "$status", "doc_last_scan": "$last_scan", "doc_created_at": "$created_at"}},
                    ]
                }
            }
        ]
        result = list(regimes_collection.aggregate(facet_pipeline))
        total = result[0]["total"][0]["count"] if result and result[0].get("total") else 0
        items_raw = result[0].get("items", []) if result else []
        items_list = []
        for row in items_raw:
            rec = dict(row.get("record") or {})
            rec.pop("id", None)
            rec.pop("status", None)
            rec["rcs_number"] = row.get("registration_id")
            rec["deposit_number"] = row.get("deposit_number")
            rec["legal_name"] = row.get("doc_legal_name") or rec.get("legal_name")
            rec["entity_status"] = row.get("doc_entity_status") or rec.get("entity_status")
            doc_created = row.get("doc_created_at")
            if doc_created and ("created_at" not in rec or not rec.get("created_at")):
                rec["created_at"] = doc_created
            rec = serialize_for_api(rec)
            items_list.append(rec)
        total_pages = math.ceil(total / page_size) if page_size else 0
        return {"items": items_list, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed during regime search: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error searching regimes: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to search regimes: {str(e)}")


@router.get("/all/regimes", summary="Get all regime data (entity-parity path)")
async def get_all_regimes_data():
    """Same response as GET /regime. Each regime includes rcs_number and deposit_number."""
    return await get_all_regime_data()


@router.get("/{rcs_number}/regime", summary="Get regime data for an RCS number")
@router.get("/{rcs_number}/regimes", summary="Get regime data for an RCS number (entity-parity path)")
async def get_regime_data(rcs_number: str):
    """
    Get regime data for a given RCS number (includes parent and child numbers).

    Returns regime data from the regimes collection.
    """
    if rcs_number.lower() == "all":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Use /api/v1/view/regime endpoint to get all regimes",
        )

    try:
        db = get_database()
        regimes_collection = db["regimes"]

        parent_pattern = re.escape(rcs_number)
        regex_pattern = f"^{parent_pattern}([-_].*)?$"

        regime_docs = list(regimes_collection.find({"registration_id": {"$regex": regex_pattern}}))

        if not regime_docs:
            logger.info(f"No regime document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "regimes": [],
                "total": 0,
                "message": f"No regime data found for RCS {rcs_number}",
            }

        regime_doc = None
        for doc in regime_docs:
            if doc.get("registration_id") == rcs_number:
                regime_doc = doc
                break
        if not regime_doc:
            regime_doc = regime_docs[0]

        all_regimes_data = {}
        for doc in regime_docs:
            doc_data = doc.get("data", {})
            if isinstance(doc_data, dict):
                for deposit_num, regime_records in doc_data.items():
                    if not isinstance(regime_records, list):
                        continue
                    if deposit_num not in all_regimes_data:
                        all_regimes_data[deposit_num] = []
                    all_regimes_data[deposit_num].extend(regime_records)

        regimes_list = []
        for deposit_number, regime_records in all_regimes_data.items():
            if not isinstance(regime_records, list):
                continue
            for regime_record in regime_records:
                if not isinstance(regime_record, dict):
                    continue
                regime_copy = dict(regime_record)
                regime_copy.pop("id", None) 
                regime_copy.pop("status", None)
                regime_copy["deposit_number"] = deposit_number
                regime_copy["rcs_number"] = regime_doc.get("registration_id")
                regime_copy["legal_name"] = regime_doc.get("legal_name")
                regime_copy["entity_status"] = regime_doc.get("entity_status")
                regime_copy = serialize_for_api(regime_copy)
                regimes_list.append(regime_copy)

        def extract_deposit_num(val):
            if not val:
                return 999999999
            match = re.search(r"L(\d+)", str(val))
            if match:
                return int(match.group(1))
            num_match = re.search(r"(\d+)", str(val))
            return int(num_match.group(1)) if num_match else 999999999

        regimes_list.sort(
            key=lambda x: (
                extract_deposit_num(x.get("deposit_number", "")),
                x.get("type", ""),
            ),
            reverse=False,
        )

        last_scan = serialize_for_api(regime_doc.get("last_scan"))
        created_at = serialize_for_api(regime_doc.get("created_at"))

        if created_at:
            for r in regimes_list:
                if "created_at" not in r or not r.get("created_at"):
                    r["created_at"] = created_at

        logger.info(f"Retrieved {len(regimes_list)} regime records for RCS {rcs_number}")

        return {
            "rcs_number": rcs_number,
            "legal_name": regime_doc.get("legal_name"),
            "status": regime_doc.get("status"),
            "last_scan": last_scan,
            "regimes": regimes_list,
            "total": len(regimes_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving regime data for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve regime data: {str(e)}",
        )

# ================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
