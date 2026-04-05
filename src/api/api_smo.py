"""
API endpoints for SMO (Senior Management Officials) data

API parity with entity: rcs-numbers, search, all/smos.
"""
import logging
import math
import re
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status
from pymongo.errors import OperationFailure

from ..access.mongodb import get_database
from ..services.svc_common import extract_number_from_deposit_number
from ..utils.json_utils import serialize_for_api

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/view", tags=["SMO"])

_BUILDING_NUMBER_PATTERN = re.compile(r"^\d+[A-Za-z0-9\s\-/]*$")


def _looks_like_building_number(value: str) -> bool:
    """Heuristic check to determine if a string represents a building number."""
    if not isinstance(value, str):
        return False
    normalized = value.strip()
    if not normalized:
        return False
    return bool(_BUILDING_NUMBER_PATTERN.match(normalized))


def _normalize_smo_record(rec: dict) -> dict:
    """Normalize SMO record: map legacy shareholder_country -> smo_country."""
    if "shareholder_country" in rec and "smo_country" not in rec:
        rec["smo_country"] = rec.pop("shareholder_country")
    return rec


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


@router.get("/smo", summary="Get all SMO data from all RCS numbers")
async def get_all_smo_data():
    """
    Get all SMO data from all RCS numbers.
    
    Returns all SMO records from all RCS numbers, with each deposit number as a separate group.
    Each SMO will include its RCS number and deposit number for identification.
    """
    try:
        db = get_database()
        smos_collection = db["smos"]
        
        all_docs = smos_collection.find({})
        
        all_smos_list = []
        
        for smo_doc in all_docs:
            rcs_number = smo_doc.get("registration_id")
            if not rcs_number:
                continue
            
            doc_created_at = serialize_for_api(smo_doc.get("created_at"))

            smos_data = smo_doc.get("data", {})
            
            if not isinstance(smos_data, dict):
                continue
            
            for deposit_number, smo_records in smos_data.items():
                if not isinstance(smo_records, list):
                    continue
                
                for smo_record in smo_records:
                    if not isinstance(smo_record, dict):
                        continue
                    
                    smo_copy = dict(smo_record)
                    smo_copy["deposit_number"] = deposit_number
                    smo_copy["rcs_number"] = rcs_number
                    _normalize_smo_record(smo_copy)
                    if doc_created_at and ("created_at" not in smo_copy or not smo_copy["created_at"]):
                        smo_copy["created_at"] = doc_created_at
                    smo_copy = serialize_for_api(smo_copy)
                    normalized_address = _normalize_professional_address(smo_copy.get("professional_address"))
                    if normalized_address is not None:
                        smo_copy["professional_address"] = normalized_address
                    
                    all_smos_list.append(smo_copy)
        
        all_smos_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                x.get("deposit_number", ""),
                x.get("smo_name", "")
            ),
            reverse=False
        )
        
        logger.info(f"Retrieved {len(all_smos_list)} total SMO records from all RCS numbers")

        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": "success",
            "last_scan": None,
            "smos": all_smos_list,
            "total": len(all_smos_list)
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error retrieving all SMO data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve all SMO data: {str(e)}"
        )


@router.get("/smos/rcs-numbers", summary="Get all RCS numbers with SMO data")
async def get_smos_rcs_numbers():
    """
    Get all RCS numbers that have SMO data in the database.

    Returns a list of RCS numbers with metadata from the smos collection.
    """
    try:
        db = get_database()
        smos_collection = db["smos"]

        rcs_numbers = smos_collection.distinct("registration_id")

        logger.info(f"Found {len(rcs_numbers)} distinct RCS numbers with SMO data")

        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            try:
                smo_doc = smos_collection.find_one({"registration_id": rcs_number})
                if smo_doc:
                    smos_data = smo_doc.get("data", {})
                    if not isinstance(smos_data, dict):
                        smos_data = {}

                    total_records = sum(
                        len(recs) for recs in smos_data.values()
                        if isinstance(recs, list)
                    )
                    last_scan = serialize_for_api(smo_doc.get("last_scan"))

                    rcs_list.append({
                        "rcs_number": rcs_number,
                        "legal_name": smo_doc.get("legal_name"),
                        "status": smo_doc.get("status"),
                        "last_scan": last_scan,
                        "total_records": total_records,
                    })
            except Exception as e:
                logger.warning(f"Error processing RCS {rcs_number}: {e}")
                rcs_list.append({
                    "rcs_number": rcs_number,
                    "legal_name": None,
                    "status": None,
                    "last_scan": None,
                    "total_records": 0,
                })

        return {"total": len(rcs_list), "rcs_numbers": rcs_list}

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error retrieving SMO RCS numbers: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve SMO RCS numbers: {str(e)}"
        )
_SMO_RECORD_FIELDS = (
    "smo_name", "smo_type", "registration_no", "category", "professional_address",
    "function", "signing_authority", "smo_country", "place_of_birth",
    "country_of_birth", "permanent_representative", "registration_authority",
    "governing_body", "mandate_duration", "mandate_expiration_agm", "change_status",
    "jurisdiction",
)

_SMO_Q_SEARCH_FIELDS = (
    "smo_name", "professional_address", "function", "registration_no",
    "registration_authority", "governing_body", "permanent_representative",
    "place_of_birth", "country_of_birth", "smo_country", "shareholder_country",
    "smo_type",
    "category", "signing_authority", "mandate_duration", "mandate_expiration_agm",
)


def _build_smo_search_query(params: dict, q: Optional[str] = None) -> list:
    """Build MongoDB aggregation stages for SMO search (list-based data per deposit)."""
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
            "dataArr": {"$objectToArray": {"$ifNull": ["$data", {}]}}
        }
    })
    stages.append({"$unwind": {"path": "$dataArr", "preserveNullAndEmptyArrays": False}})
    stages.append({"$unwind": {"path": "$dataArr.v", "preserveNullAndEmptyArrays": False}})

    record_match = {}
    v_prefix = "dataArr.v."
    for field in _SMO_RECORD_FIELDS:
        val = params.get(field)
        if val is not None and str(val).strip():
            regex_val = {"$regex": re.escape(str(val).strip()), "$options": "i"}
            if field == "smo_country":
                record_match["$and"] = record_match.get("$and", []) + [
                    {"$or": [
                        {v_prefix + "smo_country": regex_val},
                        {v_prefix + "shareholder_country": regex_val},
                    ]}
                ]
            else:
                record_match[v_prefix + field] = regex_val

    if q and q.strip():
        q_esc = re.escape(q.strip())
        regex = {"$regex": q_esc, "$options": "i"}
        q_or = {"$or": [{v_prefix + f: regex} for f in _SMO_Q_SEARCH_FIELDS]}
        record_match["$and"] = record_match.get("$and", []) + [q_or]

    if record_match:
        match_expr = record_match
        if "$and" in record_match:
            and_list = record_match["$and"]
            rest = {k: v for k, v in record_match.items() if k != "$and"}
            match_expr = {"$and": [rest] + and_list} if rest else ({"$and": and_list} if len(and_list) > 1 else and_list[0])
        stages.append({"$match": match_expr})
    return stages


@router.get("/smos/search", summary="Search SMOs with pagination")
@router.get("/smos/deposits/search", summary="Search all SMO records with pagination (entity-parity)")
async def search_smos(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    smo_name: Optional[str] = Query(None, description="Filter by SMO name"),
    smo_type: Optional[str] = Query(None, description="Filter by SMO type"),
    registration_no: Optional[str] = Query(None, description="Filter by registration number"),
    category: Optional[str] = Query(None, description="Filter by category"),
    professional_address: Optional[str] = Query(None, description="Filter by professional address"),
    function: Optional[str] = Query(None, description="Filter by function"),
    signing_authority: Optional[str] = Query(None, description="Filter by signing authority"),
    smo_country: Optional[str] = Query(None, description="Filter by SMO country"),
    place_of_birth: Optional[str] = Query(None, description="Filter by place of birth"),
    country_of_birth: Optional[str] = Query(None, description="Filter by country of birth"),
    registration_authority: Optional[str] = Query(None, description="Filter by registration authority"),
    governing_body: Optional[str] = Query(None, description="Filter by governing body"),
    mandate_duration: Optional[str] = Query(None, description="Filter by mandate duration"),
    mandate_expiration_agm: Optional[str] = Query(None, description="Filter by mandate expiration AGM"),
    permanent_representative: Optional[str] = Query(None, description="Filter by permanent representative"),
    change_status: Optional[str] = Query(None, description="Filter by change status"),
    jurisdiction: Optional[str] = Query(None, description="Filter by jurisdiction"),
    q: Optional[str] = Query(None, description="Full-text search across all text fields (smo_name, professional_address, function, etc.)"),
):
    """
    Search SMOs by any combination of filters, with pagination.

    All filter parameters map to SMODocument fields. Returns full SMO records including
    rcs_number, deposit_number, and doc-level legal_name, last_scan, status.
    """
    try:
        params = {
            "registration_id": registration_id,
            "legal_name": legal_name,
            "smo_name": smo_name,
            "smo_type": smo_type,
            "registration_no": registration_no,
            "category": category,
            "professional_address": professional_address,
            "function": function,
            "signing_authority": signing_authority,
            "smo_country": smo_country,
            "place_of_birth": place_of_birth,
            "country_of_birth": country_of_birth,
            "registration_authority": registration_authority,
            "governing_body": governing_body,
            "mandate_duration": mandate_duration,
            "mandate_expiration_agm": mandate_expiration_agm,
            "permanent_representative": permanent_representative,
            "change_status": change_status,
            "jurisdiction": jurisdiction,
        }
        db = get_database()
        smos_collection = db["smos"]

        base_pipeline = _build_smo_search_query(params, q)
        skip = (page - 1) * page_size

        facet_pipeline = base_pipeline + [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "items": [
                        {"$sort": {"registration_id": 1, "dataArr.k": 1, "dataArr.v.smo_name": 1}},
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
                            }
                        },
                    ]
                }
            }
        ]

        cursor = smos_collection.aggregate(facet_pipeline)
        result = list(cursor)

        total = 0
        if result and result[0].get("total"):
            total = result[0]["total"][0].get("count", 0)

        items_raw = result[0].get("items", []) if result else []
        items_list = []
        for row in items_raw:
            rec = dict(row.get("record") or {})
            rec["rcs_number"] = row.get("registration_id")
            rec["deposit_number"] = row.get("deposit_number")
            _normalize_smo_record(rec)
            if row.get("doc_legal_name") is not None and not rec.get("legal_name"):
                rec["legal_name"] = row.get("doc_legal_name")
            if row.get("doc_status") is not None and not rec.get("status"):
                rec["status"] = row.get("doc_status")
            if row.get("doc_last_scan") is not None and not rec.get("last_scan"):
                rec["last_scan"] = row.get("doc_last_scan")
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

        return {
            "items": items_list,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed during SMO search: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error searching SMOs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search SMOs: {str(e)}"
        )


@router.get("/all/smos", summary="Get all SMO data (entity-parity path)")
async def get_all_smos_data():
    """
    Get all SMO data from all RCS numbers (alternate path for API parity with entities).

    Same response as GET /smo. Each SMO includes rcs_number and deposit_number.
    """
    return await get_all_smo_data()


@router.get("/{rcs_number}/smo", summary="Get SMO data for an RCS number")
@router.get("/{rcs_number}/smos", summary="Get SMO data for an RCS number (entity-parity path)")
async def get_smo_data(rcs_number: str):
    """
    Get SMO data for a given RCS number (includes parent and child numbers).
    
    Returns SMO data from the smos collection, organized by deposit number.
    Structure: ONE document per RCS, SMO records nested under data[deposit_number] as arrays.
    When a parent RCS number is provided, also returns child SMOs (e.g., B215183-1, B215183_A).
    """
    if rcs_number.lower() == "all":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Use /api/v1/view/smo or /api/v1/view/all/smos endpoint to get all SMOs"
        )
    
    try:
        db = get_database()
        smos_collection = db["smos"]
        
        parent_pattern = re.escape(rcs_number)
        regex_pattern = f"^{parent_pattern}([-_].*)?$"
        
        smo_docs = list(smos_collection.find({"registration_id": {"$regex": regex_pattern}}))
        
        if not smo_docs:
            logger.info(f"No SMO document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "smos": [],
                "total": 0,
                "message": f"No SMO data found for RCS {rcs_number}"
            }
        
        smo_doc = None
        for doc in smo_docs:
            if doc.get("registration_id") == rcs_number:
                smo_doc = doc
                break
        if not smo_doc:
            smo_doc = smo_docs[0]
        
        all_smos_data = {}
        for doc in smo_docs:
            doc_data = doc.get("data", {})
            if isinstance(doc_data, dict):
                for deposit_num, smo_records in doc_data.items():
                    if not isinstance(smo_records, list):
                        continue
                    if deposit_num not in all_smos_data:
                        all_smos_data[deposit_num] = []
                    all_smos_data[deposit_num].extend(smo_records)
        
        if not smo_doc:
            logger.info(f"No SMO document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "smos": [],
                "total": 0,
                "message": f"No SMO data found for RCS {rcs_number}"
            }
        
        smos_data = all_smos_data
        
        if not isinstance(smos_data, dict):
            logger.warning(f"Invalid data structure for RCS {rcs_number}: data field is not a dict")
            smos_data = {}
        
        smos_list = []
        
        for deposit_number, smo_records in smos_data.items():
            if not isinstance(smo_records, list):
                logger.warning(f"Invalid SMO records for deposit {deposit_number} in RCS {rcs_number}")
                continue
            
            for smo_record in smo_records:
                if not isinstance(smo_record, dict):
                    logger.warning(f"Invalid SMO record in deposit {deposit_number} for RCS {rcs_number}")
                    continue
                smo_copy = dict(smo_record)
                smo_copy["deposit_number"] = deposit_number
                _normalize_smo_record(smo_copy)
                smo_copy = serialize_for_api(smo_copy)
                normalized_address = _normalize_professional_address(smo_copy.get("professional_address"))
                if normalized_address is not None:
                    smo_copy["professional_address"] = normalized_address
                smos_list.append(smo_copy)
        
        def extract_deposit_number(deposit_num):
            """Extract numeric part from deposit number for sorting"""
            if not deposit_num:
                return 999999999
            match = re.search(r'L(\d+)', str(deposit_num))
            if match:
                return int(match.group(1))
            num_match = re.search(r'(\d+)', str(deposit_num))
            return int(num_match.group(1)) if num_match else 999999999
        
        smos_list.sort(
            key=lambda x: (
                extract_deposit_number(x.get("deposit_number", "")),
                x.get("smo_name", "")
            ),
            reverse=False
        )
        
        last_scan = serialize_for_api(smo_doc.get("last_scan"))
        created_at = serialize_for_api(smo_doc.get("created_at"))
        updated_at = serialize_for_api(smo_doc.get("updated_at"))
        
        if created_at:
            for smo in smos_list:
                if "created_at" not in smo or not smo["created_at"]:
                    smo["created_at"] = created_at
        
        logger.info(f"Retrieved {len(smos_list)} SMO records for RCS {rcs_number}")
        
        return {
            "rcs_number": rcs_number,
            "legal_name": smo_doc.get("legal_name"),
            "status": smo_doc.get("status"),
            "last_scan": last_scan,
            "created_at": created_at,
            "updated_at": updated_at,
            "smos": smos_list,
            "total": len(smos_list)
        }
        
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error retrieving SMO data for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve SMO data: {str(e)}"
        )

# ================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
