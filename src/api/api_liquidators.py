"""
API endpoints for Liquidators data
API parity with entity/SMO: rcs-numbers, search, deposits/search, all/liquidators.
"""
import logging
import math
import re
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Query, status
from pymongo.errors import OperationFailure

from ..access.mongodb import get_database
from ..utils.json_utils import serialize_for_api

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/view", tags=["Liquidators"])

_LIQUIDATOR_RECORD_FIELDS = (
    "liquidator_name", "liquidator_type", "registration_no", "registration_authority",
    "legal_form", "professional_address", "signature_authority", "permanent_representative",
    "liquidation_decision_date", "liquidator_general_powers", "date_of_birth",
    "place_of_birth", "country_of_birth", "country_of_residence", "change_status",
    "jurisdiction",
)

_LIQUIDATOR_Q_SEARCH_FIELDS = (
    "liquidator_name", "liquidator_type", "registration_no", "professional_address",
    "registration_authority", "signature_authority", "permanent_representative",
    "place_of_birth", "country_of_birth", "country_of_residence",
)

_BUILDING_NUMBER_PATTERN = re.compile(r"^\d+[A-Za-z0-9\s\-/]*$")


def _looks_like_building_number(value: str) -> bool:
    """Heuristic check to determine if a string represents a building number."""
    if not isinstance(value, str):
        return False
    normalized = value.strip()
    if not normalized:
        return False
    return bool(_BUILDING_NUMBER_PATTERN.match(normalized))


def _normalize_professional_address(address: Any) -> Any:
    """
    Ensure address follows the correct format: [Number], [Street], [Building], [Floor], [City], [Post Code], [Country]
    For legacy records stored incorrectly, swap the first two components when the second one looks like a building number.
    """
    if not isinstance(address, str):
        return address

    parts = [part.strip() for part in address.split(",") if part and part.strip()]
    if len(parts) < 2:
        return address.strip()

    first, second = parts[0], parts[1]
    if _looks_like_building_number(first):
        return ", ".join(parts)

    if _looks_like_building_number(second) and not _looks_like_building_number(first):
        parts[0], parts[1] = second, first
        return ", ".join(parts)

    return ", ".join(parts)


def _sanitize_liquidator_record(record: dict) -> dict:
    """Remove Deletion field from liquidator record (not saved going forward, exclude from display)."""
    copy = dict(record)
    copy.pop("Deletion", None)
    return copy


@router.get("/liquidator", summary="Get all liquidator data from all RCS numbers")
async def get_all_liquidator_data():
    """
    Get all liquidator data from all RCS numbers.

    Returns all liquidator records from all RCS numbers.
    Each liquidator includes rcs_number and deposit_number for identification.
    Deletion field is excluded from display.
    """
    try:
        db = get_database()
        liquidators_collection = db["liquidators"]

        all_docs = liquidators_collection.find({})
        all_liquidators_list = []

        for liquidator_doc in all_docs:
            rcs_number = liquidator_doc.get("registration_id")
            if not rcs_number:
                continue

            doc_created_at = serialize_for_api(liquidator_doc.get("created_at"))
            liquidators_data = liquidator_doc.get("data", {})

            if not isinstance(liquidators_data, dict):
                continue

            for deposit_number, liquidator_records in liquidators_data.items():
                if not isinstance(liquidator_records, list):
                    continue

                for liquidator_record in liquidator_records:
                    if not isinstance(liquidator_record, dict):
                        continue

                    liquidator_copy = _sanitize_liquidator_record(liquidator_record)
                    liquidator_copy["deposit_number"] = deposit_number
                    liquidator_copy["rcs_number"] = rcs_number

                    if doc_created_at and ("created_at" not in liquidator_copy or not liquidator_copy["created_at"]):
                        liquidator_copy["created_at"] = doc_created_at

                    liquidator_copy = serialize_for_api(liquidator_copy)
                    normalized_address = _normalize_professional_address(liquidator_copy.get("professional_address"))
                    if normalized_address is not None:
                        liquidator_copy["professional_address"] = normalized_address

                    all_liquidators_list.append(liquidator_copy)

        all_liquidators_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                x.get("deposit_number", ""),
                x.get("liquidator_name", "")
            ),
            reverse=False
        )

        logger.info(f"Retrieved {len(all_liquidators_list)} total liquidator records from all RCS numbers")

        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": "success",
            "last_scan": None,
            "liquidators": all_liquidators_list,
            "total": len(all_liquidators_list)
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error retrieving all liquidator data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve all liquidator data: {str(e)}"
        )


def _build_liquidator_search_query(params: dict, q: Optional[str] = None) -> list:
    """Build MongoDB aggregation stages for liquidator search."""
    root_match = {}
    for field in ("registration_id", "legal_name"):
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
    for field in _LIQUIDATOR_RECORD_FIELDS:
        val = params.get(field)
        if val is not None and str(val).strip():
            record_match[v_prefix + field] = {"$regex": re.escape(str(val).strip()), "$options": "i"}
    if q and q.strip():
        q_esc = re.escape(q.strip())
        regex = {"$regex": q_esc, "$options": "i"}
        record_match["$or"] = [{v_prefix + f: regex} for f in _LIQUIDATOR_Q_SEARCH_FIELDS]
    if record_match:
        stages.append({"$match": record_match})
    return stages


@router.get("/liquidators/rcs-numbers", summary="Get all RCS numbers with liquidator data")
async def get_liquidators_rcs_numbers():
    """Get all RCS numbers that have liquidator data."""
    try:
        db = get_database()
        liquidators_collection = db["liquidators"]
        rcs_numbers = liquidators_collection.distinct("registration_id")
        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            try:
                doc = liquidators_collection.find_one({"registration_id": rcs_number})
                if doc:
                    data = doc.get("data", {}) or {}
                    total_records = sum(len(r) for r in data.values() if isinstance(r, list))
                    rcs_list.append({
                        "rcs_number": rcs_number,
                        "legal_name": doc.get("legal_name"),
                        "status": doc.get("status"),
                        "last_scan": serialize_for_api(doc.get("last_scan")),
                        "total_records": total_records,
                    })
                else:
                    rcs_list.append({"rcs_number": rcs_number, "legal_name": None, "status": None, "last_scan": None, "total_records": 0})
            except Exception as e:
                logger.warning(f"Error processing RCS {rcs_number}: {e}")
                rcs_list.append({"rcs_number": rcs_number, "legal_name": None, "status": None, "last_scan": None, "total_records": 0})
        return {"total": len(rcs_list), "rcs_numbers": rcs_list}
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error retrieving liquidator RCS numbers: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve liquidator RCS numbers: {str(e)}")


@router.get("/liquidators/search", summary="Search liquidators with pagination")
@router.get("/liquidators/deposits/search", summary="Search all liquidator records with pagination (entity-parity)")
async def search_liquidators(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    liquidator_name: Optional[str] = Query(None, description="Filter by liquidator name"),
    liquidator_type: Optional[str] = Query(None, description="Filter by liquidator type"),
    registration_no: Optional[str] = Query(None, description="Filter by registration number"),
    professional_address: Optional[str] = Query(None, description="Filter by professional address"),
    change_status: Optional[str] = Query(None, description="Filter by change status"),
    jurisdiction: Optional[str] = Query(None, description="Filter by jurisdiction"),
    q: Optional[str] = Query(None, description="Full-text search across main text fields"),
):
    """Search liquidators by filters with pagination. Deletion field excluded from response."""
    try:
        params = {
            "registration_id": registration_id, "legal_name": legal_name, "liquidator_name": liquidator_name,
            "liquidator_type": liquidator_type, "registration_no": registration_no,
            "professional_address": professional_address, "change_status": change_status, "jurisdiction": jurisdiction,
        }
        db = get_database()
        liquidators_collection = db["liquidators"]
        base_pipeline = _build_liquidator_search_query(params, q)
        skip = (page - 1) * page_size
        facet_pipeline = base_pipeline + [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "items": [
                        {"$sort": {"registration_id": 1, "dataArr.k": 1, "dataArr.v.liquidator_name": 1}},
                        {"$skip": skip},
                        {"$limit": page_size},
                        {"$project": {"_id": 0, "registration_id": 1, "deposit_number": "$dataArr.k", "record": "$dataArr.v",
                                      "doc_legal_name": "$legal_name", "doc_status": "$status", "doc_last_scan": "$last_scan",
                                      "doc_created_at": "$created_at", "doc_updated_at": "$updated_at"}},
                    ]
                }
            }
        ]
        result = list(liquidators_collection.aggregate(facet_pipeline))
        total = result[0]["total"][0]["count"] if result and result[0].get("total") else 0
        items_raw = result[0].get("items", []) if result else []
        items_list = []
        for row in items_raw:
            rec = _sanitize_liquidator_record(row.get("record") or {})
            rec["rcs_number"] = row.get("registration_id")
            rec["deposit_number"] = row.get("deposit_number")
            if row.get("doc_legal_name") is not None and not rec.get("legal_name"):
                rec["legal_name"] = row["doc_legal_name"]
            if row.get("doc_status") is not None and not rec.get("status"):
                rec["status"] = row["doc_status"]
            if row.get("doc_last_scan") is not None and not rec.get("last_scan"):
                rec["last_scan"] = row["doc_last_scan"]
            doc_created = row.get("doc_created_at")
            if doc_created and ("created_at" not in rec or not rec.get("created_at")):
                rec["created_at"] = doc_created
            doc_updated = row.get("doc_updated_at")
            if doc_updated and ("updated_at" not in rec or not rec.get("updated_at")):
                rec["updated_at"] = doc_updated
            rec = serialize_for_api(rec)
            normalized = _normalize_professional_address(rec.get("professional_address"))
            if normalized is not None:
                rec["professional_address"] = normalized
            items_list.append(rec)
        total_pages = math.ceil(total / page_size) if page_size else 0
        return {"items": items_list, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed during liquidator search: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error searching liquidators: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to search liquidators: {str(e)}")


@router.get("/all/liquidators", summary="Get all liquidator data (entity-parity path)")
async def get_all_liquidators_data():
    """Same response as GET /liquidator. Each liquidator includes rcs_number and deposit_number."""
    return await get_all_liquidator_data()


@router.get("/{rcs_number}/liquidator", summary="Get liquidator data for an RCS number")
@router.get("/{rcs_number}/liquidators", summary="Get liquidator data for an RCS number (entity-parity path)")
async def get_liquidator_data(rcs_number: str):
    """
    Get liquidator data for a given RCS number (includes parent and child numbers).

    Returns liquidator data from the liquidators collection.
    Deletion field is excluded from display.
    """
    if rcs_number.lower() == "all":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Use /api/v1/view/liquidator endpoint to get all liquidators"
        )

    try:
        db = get_database()
        liquidators_collection = db["liquidators"]

        parent_pattern = re.escape(rcs_number)
        regex_pattern = f"^{parent_pattern}([-_].*)?$"

        liquidator_docs = list(liquidators_collection.find({"registration_id": {"$regex": regex_pattern}}))

        if not liquidator_docs:
            logger.info(f"No liquidator document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "liquidators": [],
                "total": 0,
                "message": f"No liquidator data found for RCS {rcs_number}"
            }

        liquidator_doc = None
        for doc in liquidator_docs:
            if doc.get("registration_id") == rcs_number:
                liquidator_doc = doc
                break
        if not liquidator_doc:
            liquidator_doc = liquidator_docs[0]

        all_liquidators_data = {}
        for doc in liquidator_docs:
            doc_data = doc.get("data", {})
            if isinstance(doc_data, dict):
                for deposit_num, liquidator_records in doc_data.items():
                    if not isinstance(liquidator_records, list):
                        continue
                    if deposit_num not in all_liquidators_data:
                        all_liquidators_data[deposit_num] = []
                    all_liquidators_data[deposit_num].extend(liquidator_records)

        liquidators_list = []

        for deposit_number, liquidator_records in all_liquidators_data.items():
            if not isinstance(liquidator_records, list):
                continue

            for liquidator_record in liquidator_records:
                if not isinstance(liquidator_record, dict):
                    continue

                liquidator_copy = _sanitize_liquidator_record(liquidator_record)
                liquidator_copy["deposit_number"] = deposit_number

                liquidator_copy = serialize_for_api(liquidator_copy)

                # Ensure professional address displays in correct format
                normalized_address = _normalize_professional_address(liquidator_copy.get("professional_address"))
                if normalized_address is not None:
                    liquidator_copy["professional_address"] = normalized_address

                liquidators_list.append(liquidator_copy)

        def extract_deposit_number(deposit_num):
            if not deposit_num:
                return 999999999
            match = re.search(r"L(\d+)", str(deposit_num))
            if match:
                return int(match.group(1))
            num_match = re.search(r"(\d+)", str(deposit_num))
            return int(num_match.group(1)) if num_match else 999999999

        liquidators_list.sort(
            key=lambda x: (
                extract_deposit_number(x.get("deposit_number", "")),
                x.get("liquidator_name", "")
            ),
            reverse=False
        )

        last_scan = serialize_for_api(liquidator_doc.get("last_scan"))
        created_at = serialize_for_api(liquidator_doc.get("created_at"))
        updated_at = serialize_for_api(liquidator_doc.get("updated_at"))

        if created_at:
            for liquidator in liquidators_list:
                if "created_at" not in liquidator or not liquidator["created_at"]:
                    liquidator["created_at"] = created_at

        logger.info(f"Retrieved {len(liquidators_list)} liquidator records for RCS {rcs_number}")

        return {
            "rcs_number": rcs_number,
            "legal_name": liquidator_doc.get("legal_name"),
            "status": liquidator_doc.get("status"),
            "last_scan": last_scan,
            "created_at": created_at,
            "updated_at": updated_at,
            "liquidators": liquidators_list,
            "total": len(liquidators_list)
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error retrieving liquidator data for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve liquidator data: {str(e)}"
        )

# ================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
