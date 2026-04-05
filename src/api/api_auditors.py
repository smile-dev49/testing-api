"""
API endpoints for Auditors data
API parity with entity/SMO: rcs-numbers, search, deposits/search, all/auditors.
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

router = APIRouter(prefix="/api/v1/view", tags=["Auditors"])

_AUDITOR_RECORD_FIELDS = (
    "auditor_name", "auditor_type", "registration_no", "professional_address",
    "type_of_mandate", "mandate_duration", "mandate_expiration_agm",
    "permanent_representative", "date_of_birth", "place_of_birth", "country_of_birth",
    "changed_status", "jurisdiction",
)

_AUDITOR_Q_SEARCH_FIELDS = (
    "auditor_name", "auditor_type", "registration_no", "professional_address",
    "type_of_mandate", "mandate_duration", "permanent_representative",
    "place_of_birth", "country_of_birth",
)


@router.get("/auditor", summary="Get all auditor data from all RCS numbers")
async def get_all_auditor_data():
    """
    Get all auditor data from all RCS numbers.

    Returns all auditor records from all RCS numbers.
    Each auditor includes rcs_number, deposit_number, and changed_status for display.
    """
    try:
        db = get_database()
        auditors_collection = db["auditors"]

        all_docs = auditors_collection.find({})
        all_auditors_list = []

        for auditor_doc in all_docs:
            rcs_number = auditor_doc.get("registration_id")
            if not rcs_number:
                continue

            doc_created_at = serialize_for_api(auditor_doc.get("created_at"))
            auditors_data = auditor_doc.get("data", {})

            if not isinstance(auditors_data, dict):
                continue

            for deposit_number, auditor_records in auditors_data.items():
                if not isinstance(auditor_records, list):
                    continue

                for auditor_record in auditor_records:
                    if not isinstance(auditor_record, dict):
                        continue

                    auditor_copy = dict(auditor_record)
                    auditor_copy["deposit_number"] = deposit_number
                    auditor_copy["rcs_number"] = rcs_number

                    if doc_created_at and ("created_at" not in auditor_copy or not auditor_copy["created_at"]):
                        auditor_copy["created_at"] = doc_created_at

                    auditor_copy = serialize_for_api(auditor_copy)
                    all_auditors_list.append(auditor_copy)

        all_auditors_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                x.get("deposit_number", ""),
                x.get("auditor_name", ""),
            ),
            reverse=False,
        )

        logger.info(f"Retrieved {len(all_auditors_list)} total auditor records from all RCS numbers")

        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": "success",
            "last_scan": None,
            "auditors": all_auditors_list,
            "total": len(all_auditors_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving all auditor data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve all auditor data: {str(e)}",
        )


def _build_auditor_search_query(params: dict, q: Optional[str] = None) -> list:
    """Build MongoDB aggregation stages for auditor search."""
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
    extra_and = []
    for field in _AUDITOR_RECORD_FIELDS:
        val = params.get(field)
        if val is not None and str(val).strip():
            regex_val = {"$regex": re.escape(str(val).strip()), "$options": "i"}
            if field == "changed_status":
                extra_and.append({"$or": [
                    {v_prefix + "changed_status": regex_val},
                    {v_prefix + "change_status": regex_val},
                ]})
            else:
                record_match[v_prefix + field] = regex_val
    if q and q.strip():
        q_esc = re.escape(q.strip())
        regex = {"$regex": q_esc, "$options": "i"}
        extra_and.append({"$or": [{v_prefix + f: regex} for f in _AUDITOR_Q_SEARCH_FIELDS]})
    if extra_and:
        all_cond = ([record_match] if record_match else []) + extra_and
        record_match = {"$and": all_cond} if len(all_cond) > 1 else all_cond[0]
    if record_match:
        stages.append({"$match": record_match})
    return stages


@router.get("/auditors/rcs-numbers", summary="Get all RCS numbers with auditor data")
async def get_auditors_rcs_numbers():
    """Get all RCS numbers that have auditor data."""
    try:
        db = get_database()
        auditors_collection = db["auditors"]
        rcs_numbers = auditors_collection.distinct("registration_id")
        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            try:
                doc = auditors_collection.find_one({"registration_id": rcs_number})
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
        logger.error(f"Error retrieving auditor RCS numbers: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve auditor RCS numbers: {str(e)}")


@router.get("/auditors/search", summary="Search auditors with pagination")
@router.get("/auditors/deposits/search", summary="Search all auditor records with pagination (entity-parity)")
async def search_auditors(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    auditor_name: Optional[str] = Query(None, description="Filter by auditor name"),
    auditor_type: Optional[str] = Query(None, description="Filter by auditor type"),
    registration_no: Optional[str] = Query(None, description="Filter by registration number"),
    professional_address: Optional[str] = Query(None, description="Filter by professional address"),
    changed_status: Optional[str] = Query(None, description="Filter by changed status (added/updated/deleted)"),
    jurisdiction: Optional[str] = Query(None, description="Filter by jurisdiction"),
    q: Optional[str] = Query(None, description="Full-text search across main text fields"),
):
    """Search auditors by filters with pagination."""
    try:
        params = {
            "registration_id": registration_id, "legal_name": legal_name, "auditor_name": auditor_name,
            "auditor_type": auditor_type, "registration_no": registration_no,
            "professional_address": professional_address, "changed_status": changed_status, "jurisdiction": jurisdiction,
        }
        db = get_database()
        auditors_collection = db["auditors"]
        base_pipeline = _build_auditor_search_query(params, q)
        skip = (page - 1) * page_size
        facet_pipeline = base_pipeline + [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "items": [
                        {"$sort": {"registration_id": 1, "dataArr.k": 1, "dataArr.v.auditor_name": 1}},
                        {"$skip": skip},
                        {"$limit": page_size},
                        {"$project": {"_id": 0, "registration_id": 1, "deposit_number": "$dataArr.k", "record": "$dataArr.v",
                                      "doc_legal_name": "$legal_name", "doc_status": "$status", "doc_last_scan": "$last_scan",
                                      "doc_created_at": "$created_at", "doc_updated_at": "$updated_at"}},
                    ]
                }
            }
        ]
        result = list(auditors_collection.aggregate(facet_pipeline))
        total = result[0]["total"][0]["count"] if result and result[0].get("total") else 0
        items_raw = result[0].get("items", []) if result else []
        items_list = []
        for row in items_raw:
            rec = dict(row.get("record") or {})
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
            items_list.append(rec)
        total_pages = math.ceil(total / page_size) if page_size else 0
        return {"items": items_list, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed during auditor search: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error searching auditors: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to search auditors: {str(e)}")


@router.get("/all/auditors", summary="Get all auditor data (entity-parity path)")
async def get_all_auditors_data():
    """Same response as GET /auditor. Each auditor includes rcs_number and deposit_number."""
    return await get_all_auditor_data()


@router.get("/{rcs_number}/auditor", summary="Get auditor data for an RCS number")
@router.get("/{rcs_number}/auditors", summary="Get auditor data for an RCS number (entity-parity path)")
async def get_auditor_data(rcs_number: str):
    """
    Get auditor data for a given RCS number (includes parent and child numbers).

    Returns auditor data from the auditors collection.
    """
    if rcs_number.lower() == "all":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Use /api/v1/view/auditor endpoint to get all auditors",
        )

    try:
        db = get_database()
        auditors_collection = db["auditors"]

        parent_pattern = re.escape(rcs_number)
        regex_pattern = f"^{parent_pattern}([-_].*)?$"

        auditor_docs = list(auditors_collection.find({"registration_id": {"$regex": regex_pattern}}))

        if not auditor_docs:
            logger.info(f"No auditor document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "auditors": [],
                "total": 0,
                "message": f"No auditor data found for RCS {rcs_number}",
            }

        auditor_doc = None
        for doc in auditor_docs:
            if doc.get("registration_id") == rcs_number:
                auditor_doc = doc
                break
        if not auditor_doc:
            auditor_doc = auditor_docs[0]

        all_auditors_data = {}
        for doc in auditor_docs:
            doc_data = doc.get("data", {})
            if isinstance(doc_data, dict):
                for deposit_num, auditor_records in doc_data.items():
                    if not isinstance(auditor_records, list):
                        continue
                    if deposit_num not in all_auditors_data:
                        all_auditors_data[deposit_num] = []
                    all_auditors_data[deposit_num].extend(auditor_records)

        auditors_list = []

        for deposit_number, auditor_records in all_auditors_data.items():
            if not isinstance(auditor_records, list):
                continue

            for auditor_record in auditor_records:
                if not isinstance(auditor_record, dict):
                    continue

                auditor_copy = dict(auditor_record)
                auditor_copy["deposit_number"] = deposit_number

                auditor_copy = serialize_for_api(auditor_copy)
                auditors_list.append(auditor_copy)

        def extract_deposit_number(deposit_num):
            if not deposit_num:
                return 999999999
            match = re.search(r"L(\d+)", str(deposit_num))
            if match:
                return int(match.group(1))
            num_match = re.search(r"(\d+)", str(deposit_num))
            return int(num_match.group(1)) if num_match else 999999999

        auditors_list.sort(
            key=lambda x: (
                extract_deposit_number(x.get("deposit_number", "")),
                x.get("auditor_name", ""),
            ),
            reverse=False,
        )

        last_scan = serialize_for_api(auditor_doc.get("last_scan"))
        created_at = serialize_for_api(auditor_doc.get("created_at"))
        updated_at = serialize_for_api(auditor_doc.get("updated_at"))

        if created_at:
            for auditor in auditors_list:
                if "created_at" not in auditor or not auditor["created_at"]:
                    auditor["created_at"] = created_at

        logger.info(f"Retrieved {len(auditors_list)} auditor records for RCS {rcs_number}")

        return {
            "rcs_number": rcs_number,
            "legal_name": auditor_doc.get("legal_name"),
            "status": auditor_doc.get("status"),
            "last_scan": last_scan,
            "created_at": created_at,
            "updated_at": updated_at,
            "auditors": auditors_list,
            "total": len(auditors_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving auditor data for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve auditor data: {str(e)}",
        )

# ================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
