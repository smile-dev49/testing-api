"""
API endpoints for Depositaries data.
API parity with entity/auditors: rcs-numbers, search, deposits/search, all/depositaries.
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

router = APIRouter(prefix="/api/v1/view", tags=["Depositaries"])

_DEPOSITARY_RECORD_FIELDS = (
    "depositary_name",
    "depositary_type",
    "registration_no",
    "last_name",
    "first_name",
    "date_of_birth",
    "place_of_birth",
    "country_of_birth",
    "professional_address",
    "street",
    "building_number",
    "post_code",
    "city",
    "country",
    "governing_body",
    "function",
    "signing_authority",
    "mandate_duration",
    "appointment_renewal_date",
    "mandate_expiration_date",
    "mandate_expiration_agm",
    "permanent_representative",
    "changed_status",
    "jurisdiction",
)

_DEPOSITARY_Q_SEARCH_FIELDS = (
    "depositary_name",
    "depositary_type",
    "registration_no",
    "professional_address",
    "last_name",
    "first_name",
    "place_of_birth",
    "country_of_birth",
    "city",
    "country",
    "governing_body",
    "function",
    "signing_authority",
    "mandate_duration",
    "permanent_representative",
)


def _build_depositary_search_query(params: dict, q: Optional[str] = None) -> list:
    """Build MongoDB aggregation stages for depositary search."""
    root_match = {}
    for field in ("registration_id", "legal_name"):
        val = params.get(field)
        if val is not None and str(val).strip():
            root_match[field] = {"$regex": re.escape(str(val).strip()), "$options": "i"}

    stages = []
    if root_match:
        stages.append({"$match": root_match})
    stages.append({
        "$addFields": {
            "dataArr": {"$objectToArray": {"$ifNull": ["$data", {}]}},
        },
    })
    stages.append({"$unwind": {"path": "$dataArr", "preserveNullAndEmptyArrays": False}})
    stages.append({"$unwind": {"path": "$dataArr.v", "preserveNullAndEmptyArrays": False}})

    record_match = {}
    v_prefix = "dataArr.v."
    extra_and = []
    for field in _DEPOSITARY_RECORD_FIELDS:
        val = params.get(field)
        if val is not None and str(val).strip():
            regex_val = {"$regex": re.escape(str(val).strip()), "$options": "i"}
            if field == "changed_status":
                extra_and.append({
                    "$or": [
                        {v_prefix + "changed_status": regex_val},
                        {v_prefix + "change_status": regex_val},
                    ],
                })
            else:
                record_match[v_prefix + field] = regex_val
    if params.get("deposit_number"):
        deposit_val = str(params["deposit_number"]).strip()
        if deposit_val:
            extra_and.append({"dataArr.k": {"$regex": re.escape(deposit_val), "$options": "i"}})
    if q and q.strip():
        q_esc = re.escape(q.strip())
        regex = {"$regex": q_esc, "$options": "i"}
        extra_and.append({"$or": [{v_prefix + f: regex} for f in _DEPOSITARY_Q_SEARCH_FIELDS]})
    if extra_and:
        all_cond = ([record_match] if record_match else []) + extra_and
        record_match = {"$and": all_cond} if len(all_cond) > 1 else all_cond[0]
    if record_match:
        stages.append({"$match": record_match})
    return stages


@router.get("/depositary", summary="Get all depositary data from all RCS numbers")
async def get_all_depositary_data():
    """
    Get all depositary data from all RCS numbers.

    Returns all depositary records from all RCS numbers.
    Each depositary includes rcs_number, deposit_number, and changed_status for display.
    """
    try:
        db = get_database()
        depositaries_collection = db["depositaries"]

        all_docs = depositaries_collection.find({})
        all_depositaries_list = []

        for depositary_doc in all_docs:
            rcs_number = depositary_doc.get("registration_id")
            if not rcs_number:
                continue

            doc_created_at = serialize_for_api(depositary_doc.get("created_at"))
            depositaries_data = depositary_doc.get("data", {})

            if not isinstance(depositaries_data, dict):
                continue

            for deposit_number, depositary_records in depositaries_data.items():
                if not isinstance(depositary_records, list):
                    continue

                for depositary_record in depositary_records:
                    if not isinstance(depositary_record, dict):
                        continue

                    depositary_copy = dict(depositary_record)
                    depositary_copy["deposit_number"] = deposit_number
                    depositary_copy["rcs_number"] = rcs_number

                    if doc_created_at and ("created_at" not in depositary_copy or not depositary_copy["created_at"]):
                        depositary_copy["created_at"] = doc_created_at

                    depositary_copy = serialize_for_api(depositary_copy)
                    all_depositaries_list.append(depositary_copy)

        all_depositaries_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                x.get("deposit_number", ""),
                x.get("depositary_name", ""),
            ),
            reverse=False,
        )

        logger.info(f"Retrieved {len(all_depositaries_list)} total depositary records from all RCS numbers")

        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": "success",
            "last_scan": None,
            "depositaries": all_depositaries_list,
            "total": len(all_depositaries_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving depositary data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve depositary data: {str(e)}",
        )


@router.get("/depositaries/rcs-numbers", summary="Get all RCS numbers with depositary data")
async def get_depositaries_rcs_numbers():
    """Get all RCS numbers that have depositary data."""
    try:
        db = get_database()
        depositaries_collection = db["depositaries"]
        rcs_numbers = depositaries_collection.distinct("registration_id")
        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            doc = depositaries_collection.find_one({"registration_id": rcs_number})
            if doc:
                data = doc.get("data", {})
                if not isinstance(data, dict):
                    data = {}
                total_records = sum(
                    len(recs) for recs in data.values()
                    if isinstance(recs, list)
                )
                rcs_list.append({
                    "rcs_number": rcs_number,
                    "total_records": total_records,
                })
        return {"rcs_numbers": rcs_list}
    except Exception as e:
        logger.error(f"Error getting depositary RCS numbers: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )


@router.get("/depositaries/search", summary="Search depositaries with pagination")
@router.get("/depositaries/deposits/search", summary="Search all depositary records with pagination (entity-parity)")
async def search_depositaries(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    deposit_number: Optional[str] = Query(None, description="Filter by deposit number"),
    depositary_name: Optional[str] = Query(None, description="Filter by depositary name"),
    depositary_type: Optional[str] = Query(None, description="Filter by depositary type"),
    registration_no: Optional[str] = Query(None, description="Filter by registration number"),
    professional_address: Optional[str] = Query(None, description="Filter by professional address"),
    changed_status: Optional[str] = Query(None, description="Filter by changed status (added/updated/deleted)"),
    jurisdiction: Optional[str] = Query(None, description="Filter by jurisdiction"),
    q: Optional[str] = Query(None, description="Full-text search across main text fields"),
):
    """
    Search depositaries by any combination of filters, with pagination.

    Returns depositary records including rcs_number, deposit_number, and doc-level legal_name.
    """
    try:
        params = {
            "registration_id": registration_id,
            "legal_name": legal_name,
            "deposit_number": deposit_number,
            "depositary_name": depositary_name,
            "depositary_type": depositary_type,
            "registration_no": registration_no,
            "professional_address": professional_address,
            "changed_status": changed_status,
            "jurisdiction": jurisdiction,
        }
        db = get_database()
        depositaries_collection = db["depositaries"]
        base_pipeline = _build_depositary_search_query(params, q)
        skip = (page - 1) * page_size

        facet_pipeline = base_pipeline + [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "items": [
                        {"$sort": {"registration_id": 1, "dataArr.k": 1, "dataArr.v.depositary_name": 1}},
                        {"$skip": skip},
                        {"$limit": page_size},
                        {
                            "$project": {
                                "_id": 0,
                                "registration_id": 1,
                                "deposit_number": "$dataArr.k",
                                "record": "$dataArr.v",
                                "doc_legal_name": "$legal_name",
                                "doc_status": "$status",
                                "doc_last_scan": "$last_scan",
                                "doc_created_at": "$created_at",
                                "doc_updated_at": "$updated_at",
                            },
                        },
                    ],
                },
            },
        ]

        result = list(depositaries_collection.aggregate(facet_pipeline))
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
        return {
            "items": items_list,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed during depositary search: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error searching depositaries: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search depositaries: {str(e)}",
        )


@router.get("/all/depositaries", summary="Get all depositary data (entity-parity path)")
async def get_all_depositaries_data():
    """Same response as GET /depositary."""
    return await get_all_depositary_data()


@router.get("/{rcs_number}/depositary", summary="Get depositary data for an RCS number")
@router.get("/{rcs_number}/depositaries", summary="Get depositary data for an RCS number (entity-parity path)")
async def get_depositary_data(rcs_number: str):
    """
    Get depositary data for a given RCS number (includes parent and child numbers).

    Returns depositary data from the depositaries collection.
    """
    if rcs_number.lower() == "all":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Use /api/v1/view/depositary endpoint to get all depositaries",
        )

    try:
        db = get_database()
        depositaries_collection = db["depositaries"]

        parent_pattern = re.escape(rcs_number)
        regex_pattern = f"^{parent_pattern}([-_].*)?$"

        depositary_docs = list(
            depositaries_collection.find({"registration_id": {"$regex": regex_pattern}})
        )

        if not depositary_docs:
            logger.info(f"No depositary document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "depositaries": [],
                "total": 0,
                "message": f"No depositary data found for RCS {rcs_number}",
            }

        depositary_doc = None
        for doc in depositary_docs:
            if doc.get("registration_id") == rcs_number:
                depositary_doc = doc
                break
        if not depositary_doc:
            depositary_doc = depositary_docs[0]

        all_depositaries_data = {}
        for doc in depositary_docs:
            doc_data = doc.get("data", {})
            if isinstance(doc_data, dict):
                for deposit_num, depositary_records in doc_data.items():
                    if not isinstance(depositary_records, list):
                        continue
                    if deposit_num not in all_depositaries_data:
                        all_depositaries_data[deposit_num] = []
                    all_depositaries_data[deposit_num].extend(depositary_records)

        depositaries_list = []

        for deposit_number, depositary_records in all_depositaries_data.items():
            if not isinstance(depositary_records, list):
                continue

            for depositary_record in depositary_records:
                if not isinstance(depositary_record, dict):
                    continue

                depositary_copy = dict(depositary_record)
                depositary_copy["deposit_number"] = deposit_number

                depositary_copy = serialize_for_api(depositary_copy)
                depositaries_list.append(depositary_copy)

        def extract_deposit_number(deposit_num):
            if not deposit_num:
                return 999999999
            match = re.search(r"L(\d+)", str(deposit_num))
            if match:
                return int(match.group(1))
            num_match = re.search(r"(\d+)", str(deposit_num))
            return int(num_match.group(1)) if num_match else 999999999

        depositaries_list.sort(
            key=lambda x: (
                extract_deposit_number(x.get("deposit_number", "")),
                x.get("depositary_name", ""),
            ),
            reverse=False,
        )

        last_scan = serialize_for_api(depositary_doc.get("last_scan"))
        created_at = serialize_for_api(depositary_doc.get("created_at"))
        updated_at = serialize_for_api(depositary_doc.get("updated_at"))

        if created_at:
            for depositary in depositaries_list:
                if "created_at" not in depositary or not depositary["created_at"]:
                    depositary["created_at"] = created_at

        logger.info(f"Retrieved {len(depositaries_list)} depositary records for RCS {rcs_number}")

        return {
            "rcs_number": rcs_number,
            "legal_name": depositary_doc.get("legal_name"),
            "status": depositary_doc.get("status"),
            "last_scan": last_scan,
            "created_at": created_at,
            "updated_at": updated_at,
            "depositaries": depositaries_list,
            "total": len(depositaries_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving depositary data for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve depositary data: {str(e)}",
        )
