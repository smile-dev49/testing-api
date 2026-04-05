"""
API endpoints for Shareholders data
API parity with entity/SMO: rcs-numbers, search, deposits/search, all/shareholders.
"""
import logging
import math
import re
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status
from pymongo.errors import OperationFailure

from ..access.mongodb import get_database
from ..utils.json_utils import serialize_for_api

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/view", tags=["Shareholders"])

_SHAREHOLDER_RECORD_FIELDS = (
    "shareholder_name", "shareholder_type", "registration_no", "professional_address",
    "shareholder_country", "shareholder_legal_form", "registration_authority",
    "permanent_representative", "date_of_birth", "place_of_birth", "country_of_birth",
    "change_status", "jurisdiction",
)

_SHAREHOLDER_Q_SEARCH_FIELDS = (
    "shareholder_name", "shareholder_type", "registration_no", "professional_address",
    "shareholder_country", "shareholder_legal_form", "registration_authority",
    "permanent_representative", "place_of_birth", "country_of_birth",
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
    Ensure building number appears before street in professional addresses.
    For legacy records stored as 'Street, BuildingNumber, ...', swap the first
    two components when the second one looks like a building number.
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


@router.get("/shareholder", summary="Get all shareholder data from all RCS numbers")
async def get_all_shareholder_data():
    """
    Get all shareholder data from all RCS numbers.
    
    Returns all shareholder records from all RCS numbers, with each deposit number as a separate group.
    Each shareholder will include its RCS number and deposit number for identification.
    """
    try:
        db = get_database()
        shareholders_collection = db["shareholders"]
        
        all_docs = shareholders_collection.find({})
        
        all_shareholders_list = []
        
        for shareholder_doc in all_docs:
            rcs_number = shareholder_doc.get("registration_id")
            if not rcs_number:
                continue
            
            doc_created_at = serialize_for_api(shareholder_doc.get("created_at"))
            shareholders_data = shareholder_doc.get("data", {})
            
            if not isinstance(shareholders_data, dict):
                continue
            for deposit_number, shareholder_records in shareholders_data.items():
                if not isinstance(shareholder_records, list):
                    continue
                
                for shareholder_record in shareholder_records:
                    if not isinstance(shareholder_record, dict):
                        continue
                    
                    shareholder_copy = dict(shareholder_record)
                    shareholder_copy["deposit_number"] = deposit_number
                    shareholder_copy["rcs_number"] = rcs_number
                    if doc_created_at and ("created_at" not in shareholder_copy or not shareholder_copy["created_at"]):
                        shareholder_copy["created_at"] = doc_created_at
                    
                    shareholder_copy = serialize_for_api(shareholder_copy)
                    normalized_address = _normalize_professional_address(shareholder_copy.get("professional_address"))
                    if normalized_address is not None:
                        shareholder_copy["professional_address"] = normalized_address
                    
                    all_shareholders_list.append(shareholder_copy)
        
        all_shareholders_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                x.get("deposit_number", ""),
                x.get("shareholder_name", "")
            ),
            reverse=False
        )
        
        logger.info(f"Retrieved {len(all_shareholders_list)} total shareholder records from all RCS numbers")
        
        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": "success",
            "last_scan": None,
            "shareholders": all_shareholders_list,
            "total": len(all_shareholders_list)
        }
        
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error retrieving all shareholder data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve all shareholder data: {str(e)}"
        )


def _build_shareholder_search_query(params: dict, q: Optional[str] = None) -> list:
    """Build MongoDB aggregation stages for shareholder search."""
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
    for field in _SHAREHOLDER_RECORD_FIELDS:
        val = params.get(field)
        if val is not None and str(val).strip():
            record_match[v_prefix + field] = {"$regex": re.escape(str(val).strip()), "$options": "i"}
    if q and q.strip():
        q_esc = re.escape(q.strip())
        regex = {"$regex": q_esc, "$options": "i"}
        record_match["$or"] = [{v_prefix + f: regex} for f in _SHAREHOLDER_Q_SEARCH_FIELDS]
    if record_match:
        stages.append({"$match": record_match})
    return stages


@router.get("/shareholders/rcs-numbers", summary="Get all RCS numbers with shareholder data")
async def get_shareholders_rcs_numbers():
    """Get all RCS numbers that have shareholder data."""
    try:
        db = get_database()
        shareholders_collection = db["shareholders"]
        rcs_numbers = shareholders_collection.distinct("registration_id")
        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            try:
                doc = shareholders_collection.find_one({"registration_id": rcs_number})
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
        logger.error(f"Error retrieving shareholder RCS numbers: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve shareholder RCS numbers: {str(e)}")


@router.get("/shareholders/search", summary="Search shareholders with pagination")
@router.get("/shareholders/deposits/search", summary="Search all shareholder records with pagination (entity-parity)")
async def search_shareholders(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    shareholder_name: Optional[str] = Query(None, description="Filter by shareholder name"),
    shareholder_type: Optional[str] = Query(None, description="Filter by shareholder type"),
    registration_no: Optional[str] = Query(None, description="Filter by registration number"),
    professional_address: Optional[str] = Query(None, description="Filter by professional address"),
    shareholder_country: Optional[str] = Query(None, description="Filter by shareholder country"),
    shareholder_legal_form: Optional[str] = Query(None, description="Filter by shareholder legal form"),
    registration_authority: Optional[str] = Query(None, description="Filter by registration authority"),
    permanent_representative: Optional[str] = Query(None, description="Filter by permanent representative"),
    change_status: Optional[str] = Query(None, description="Filter by change status"),
    jurisdiction: Optional[str] = Query(None, description="Filter by jurisdiction"),
    q: Optional[str] = Query(None, description="Full-text search across main text fields"),
):
    """Search shareholders by filters with pagination. Returns full records including rcs_number, deposit_number."""
    try:
        params = {
            "registration_id": registration_id, "legal_name": legal_name, "shareholder_name": shareholder_name,
            "shareholder_type": shareholder_type, "registration_no": registration_no,
            "professional_address": professional_address, "shareholder_country": shareholder_country,
            "shareholder_legal_form": shareholder_legal_form, "registration_authority": registration_authority,
            "permanent_representative": permanent_representative, "change_status": change_status, "jurisdiction": jurisdiction,
        }
        db = get_database()
        shareholders_collection = db["shareholders"]
        base_pipeline = _build_shareholder_search_query(params, q)
        skip = (page - 1) * page_size
        facet_pipeline = base_pipeline + [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "items": [
                        {"$sort": {"registration_id": 1, "dataArr.k": 1, "dataArr.v.shareholder_name": 1}},
                        {"$skip": skip},
                        {"$limit": page_size},
                        {"$project": {"_id": 0, "registration_id": 1, "deposit_number": "$dataArr.k", "record": "$dataArr.v",
                                      "doc_legal_name": "$legal_name", "doc_status": "$status", "doc_last_scan": "$last_scan",
                                      "doc_created_at": "$created_at", "doc_updated_at": "$updated_at"}},
                    ]
                }
            }
        ]
        result = list(shareholders_collection.aggregate(facet_pipeline))
        total = result[0]["total"][0]["count"] if result and result[0].get("total") else 0
        items_raw = result[0].get("items", []) if result else []
        items_list = []
        for row in items_raw:
            rec = dict(row.get("record") or {})
            rec.pop("Deletion", None)
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
        logger.error(f"MongoDB operation failed during shareholder search: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error searching shareholders: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to search shareholders: {str(e)}")


@router.get("/all/shareholders", summary="Get all shareholder data (entity-parity path)")
async def get_all_shareholders_data():
    """Same response as GET /shareholder. Each shareholder includes rcs_number and deposit_number."""
    return await get_all_shareholder_data()


@router.get("/{rcs_number}/shareholder", summary="Get shareholder data for an RCS number")
@router.get("/{rcs_number}/shareholders", summary="Get shareholder data for an RCS number (entity-parity path)")
async def get_shareholder_data(rcs_number: str):
    """
    Get shareholder data for a given RCS number (includes parent and child numbers).
    
    Returns shareholder data from the shareholders collection, organized by deposit number.
    Structure: ONE document per RCS, shareholder records nested under data[deposit_number] as arrays.
    When a parent RCS number is provided, also returns child shareholders (e.g., B215183-1, B215183_A).
    """
    if rcs_number.lower() == "all":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Use /api/v1/view/shareholder endpoint to get all shareholders"
        )
    
    try:
        db = get_database()
        shareholders_collection = db["shareholders"]
        
        parent_pattern = re.escape(rcs_number)
        regex_pattern = f"^{parent_pattern}([-_].*)?$"
        
        shareholder_docs = list(shareholders_collection.find({"registration_id": {"$regex": regex_pattern}}))
        
        if not shareholder_docs:
            logger.info(f"No shareholder document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "shareholders": [],
                "total": 0,
                "message": f"No shareholder data found for RCS {rcs_number}"
            }
        
        shareholder_doc = None
        for doc in shareholder_docs:
            if doc.get("registration_id") == rcs_number:
                shareholder_doc = doc
                break
        if not shareholder_doc:
            shareholder_doc = shareholder_docs[0]
        
        all_shareholders_data = {}
        for doc in shareholder_docs:
            doc_data = doc.get("data", {})
            if isinstance(doc_data, dict):
                for deposit_num, shareholder_records in doc_data.items():
                    if not isinstance(shareholder_records, list):
                        continue
                    if deposit_num not in all_shareholders_data:
                        all_shareholders_data[deposit_num] = []
                    all_shareholders_data[deposit_num].extend(shareholder_records)
        
        if not shareholder_doc:
            logger.info(f"No shareholder document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "last_scan": None,
                "shareholders": [],
                "total": 0,
                "message": f"No shareholder data found for RCS {rcs_number}"
            }
        shareholders_data = all_shareholders_data
        
        if not isinstance(shareholders_data, dict):
            logger.warning(f"Invalid data structure for RCS {rcs_number}: data field is not a dict")
            shareholders_data = {}
        
        shareholders_list = []
        for deposit_number, shareholder_records in shareholders_data.items():
            if not isinstance(shareholder_records, list):
                logger.warning(f"Invalid shareholder records for deposit {deposit_number} in RCS {rcs_number}")
                continue
            for shareholder_record in shareholder_records:
                if not isinstance(shareholder_record, dict):
                    logger.warning(f"Invalid shareholder record in deposit {deposit_number} for RCS {rcs_number}")
                    continue
                shareholder_copy = dict(shareholder_record)
                shareholder_copy["deposit_number"] = deposit_number
                shareholder_copy = serialize_for_api(shareholder_copy)
                normalized_address = _normalize_professional_address(shareholder_copy.get("professional_address"))
                if normalized_address is not None:
                    shareholder_copy["professional_address"] = normalized_address
                
                shareholders_list.append(shareholder_copy)
        def extract_deposit_number(deposit_num):
            """Extract numeric part from deposit number for sorting"""
            if not deposit_num:
                return 999999999
            match = re.search(r'L(\d+)', str(deposit_num))
            if match:
                return int(match.group(1))
            num_match = re.search(r'(\d+)', str(deposit_num))
            return int(num_match.group(1)) if num_match else 999999999
        
        shareholders_list.sort(
            key=lambda x: (
                extract_deposit_number(x.get("deposit_number", "")),
                x.get("shareholder_name", "")
            ),
            reverse=False
        )
        
        last_scan = serialize_for_api(shareholder_doc.get("last_scan"))
        created_at = serialize_for_api(shareholder_doc.get("created_at"))
        updated_at = serialize_for_api(shareholder_doc.get("updated_at"))
        if created_at:
            for shareholder in shareholders_list:
                if "created_at" not in shareholder or not shareholder["created_at"]:
                    shareholder["created_at"] = created_at
        
        logger.info(f"Retrieved {len(shareholders_list)} shareholder records for RCS {rcs_number}")
        
        return {
            "rcs_number": rcs_number,
            "legal_name": shareholder_doc.get("legal_name"),
            "status": shareholder_doc.get("status"),
            "last_scan": last_scan,
            "created_at": created_at,
            "updated_at": updated_at,
            "shareholders": shareholders_list,
            "total": len(shareholders_list)
        }
        
    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error retrieving shareholder data for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve shareholder data: {str(e)}"
        )

# ================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
