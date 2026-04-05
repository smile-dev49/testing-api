"""
API endpoints for Entity data
"""

import logging
import math
import re
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from pymongo.errors import OperationFailure

from ..access.mongodb import get_database
from ..services.svc_common import extract_number_from_deposit_number
from ..utils.json_utils import serialize_for_api

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/view", tags=["Entities"])
ENTITY_SEARCH_FIELDS = [
    "registration_id",
    "company_status",
    "legal_name",
    "legal_name_abbreviation",
    "legal_form",
    "additional_mention",
    "trade_name",
    "trade_name_abbreviation",
    "address",
    "social_object",
    "type_of_capital",
    "share_capital",
    "denomination",
    "contribution_type",
    "contribution_percentage",
    "incorporation_date",
    "duration",
    "duration_end_date",
    "first_financial_period_from",
    "first_financial_period_to",
    "fiscal_year_from",
    "fiscal_year_to",
    "declared_email",
    "jurisdiction",
]


@router.get("/entity", summary="Get all entity data from all RCS numbers")
async def get_all_entity_data():
    """
    Get all entity data from all RCS numbers.

    Returns all entity records from all RCS numbers, with each deposit number as a separate entity.
    Each entity will include its RCS number for identification.
    """
    try:
        db = get_database()
        entities_collection = db["entities"]

        all_docs = entities_collection.find({})

        all_entities_list = []

        for entity_doc in all_docs:
            rcs_number = entity_doc.get("registration_id")
            if not rcs_number:
                continue

            doc_created_at = serialize_for_api(entity_doc.get("created_at"))

            top_level_company_status = entity_doc.get("company_status")

            entities_data = entity_doc.get("data", {})

            if not isinstance(entities_data, dict):
                continue

            for deposit_number, entity_record in entities_data.items():
                if not isinstance(entity_record, dict):
                    continue

                entity_copy = dict(entity_record)
                entity_copy["rcs_number"] = rcs_number
                entity_copy["deposit_number"] = deposit_number
                entity_copy.pop("filing_id", None)  
                entity_copy.pop("status", None)
                entity_copy.pop("description", None)

                if top_level_company_status is not None:
                    entity_copy["company_status"] = top_level_company_status

                if doc_created_at and ("created_at" not in entity_copy or not entity_copy["created_at"]):
                    entity_copy["created_at"] = doc_created_at

                entity_copy = serialize_for_api(entity_copy)

                all_entities_list.append(entity_copy)

        all_entities_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                x.get("filing_date") or x.get("last_scan") or "",
            ),
            reverse=False,
        )

        logger.info(f"Retrieved {len(all_entities_list)} total entity records from all RCS numbers")

        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": "success",
            "last_scan": None,
            "entities": all_entities_list,
            "total": len(all_entities_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving all entity data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve all entity data: {str(e)}",
        )


@router.get("/rcs-numbers", summary="Get all saved RCS numbers")
async def get_all_rcs_numbers():
    """
    Get all RCS numbers that have been saved in the database.

    Returns a list of RCS numbers with their metadata from the entities collection.
    """
    try:
        db = get_database()
        entities_collection = db["entities"]

        rcs_numbers = entities_collection.distinct("registration_id")

        logger.info(f"Found {len(rcs_numbers)} distinct RCS numbers in database")

        rcs_list = []
        for rcs_number in sorted(rcs_numbers):
            try:
                entity_doc = entities_collection.find_one({"registration_id": rcs_number})
                if entity_doc:
                    entities_data = entity_doc.get("data", {})
                    if not isinstance(entities_data, dict):
                        entities_data = {}

                    last_scan = serialize_for_api(entity_doc.get("last_scan"))

                    rcs_list.append(
                        {
                            "rcs_number": rcs_number,
                            "legal_name": entity_doc.get("legal_name"),
                            "status": entity_doc.get("status"),
                            "company_status": entity_doc.get("company_status"),
                            "last_scan": last_scan,
                            "total_entities": len(entities_data),
                        }
                    )
            except Exception as e:
                logger.warning(f"Error processing RCS {rcs_number}: {e}")
                rcs_list.append(
                    {
                        "rcs_number": rcs_number,
                        "legal_name": None,
                        "status": None,
                        "last_scan": None,
                        "total_entities": 0,
                        "deposit_numbers": [],
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
        logger.error(f"Error retrieving RCS numbers: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve RCS numbers: {str(e)}",
        )


def _merge_entity_versions(versions: list) -> dict:
    """
    Merge multiple entity versions (from different deposit numbers) into a single entity.
    Each field takes the value from the most recent deposit that contains it.
    Deposit numbers are ordered by their numeric part (e.g. L150172349 < L160092539).
    """
    if not versions:
        return {}
    sorted_versions = sorted(
        versions,
        key=lambda x: extract_number_from_deposit_number(x.get("k") or "") or 0,
    )
    merged = {}
    for entry in sorted_versions:
        v = entry.get("v")
        if not isinstance(v, dict):
            continue
        for key, value in v.items():
            if value is not None or key not in merged:
                merged[key] = value
    return merged


def _build_entity_search_query(params: dict, q: Optional[str] = None) -> list:
    """Build MongoDB aggregation stages: root match, add data array, unwind, entity match."""
    root_match = {}
    for field in ("registration_id", "company_status", "jurisdiction"):
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
    entity_match = {}
    v_prefix = "dataArr.v."
    for field in (
        "legal_name", "legal_name_abbreviation", "legal_form", "additional_mention",
        "trade_name", "trade_name_abbreviation", "address", "social_object",
        "type_of_capital", "share_capital", "denomination", "contribution_type",
        "contribution_percentage", "incorporation_date", "duration", "duration_end_date",
        "first_financial_period_from", "first_financial_period_to",
        "fiscal_year_from", "fiscal_year_to", "declared_email",
    ):
        val = params.get(field)
        if val is not None and str(val).strip():
            entity_match[v_prefix + field] = {"$regex": re.escape(str(val).strip()), "$options": "i"}
    if q and q.strip():
        q_esc = re.escape(q.strip())
        regex = {"$regex": q_esc, "$options": "i"}
        entity_match["$or"] = [
            {v_prefix + "legal_name": regex},
            {v_prefix + "address": regex},
            {v_prefix + "trade_name": regex},
            {v_prefix + "social_object": regex},
        ]
    if entity_match:
        stages.append({"$match": entity_match})
    return stages


@router.get("/entities/search", summary="Search entities with pagination")
async def search_entities(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    company_status: Optional[str] = Query(None, description="Filter by company status"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    legal_name_abbreviation: Optional[str] = Query(None, description="Legal name abbreviation"),
    legal_form: Optional[str] = Query(None, description="Legal form"),
    additional_mention: Optional[str] = Query(None, description="Additional mention"),
    trade_name: Optional[str] = Query(None, description="Trade name"),
    trade_name_abbreviation: Optional[str] = Query(None, description="Trade name abbreviation"),
    address: Optional[str] = Query(None, description="Address"),
    social_object: Optional[str] = Query(None, description="Social object"),
    type_of_capital: Optional[str] = Query(None, description="Type of capital"),
    share_capital: Optional[str] = Query(None, description="Share capital"),
    denomination: Optional[str] = Query(None, description="Denomination"),
    contribution_type: Optional[str] = Query(None, description="Contribution type"),
    contribution_percentage: Optional[str] = Query(None, description="Contribution percentage"),
    incorporation_date: Optional[str] = Query(None, description="Incorporation date"),
    duration: Optional[str] = Query(None, description="Duration"),
    duration_end_date: Optional[str] = Query(None, description="Duration end date"),
    first_financial_period_from: Optional[str] = Query(None, description="First financial period from"),
    first_financial_period_to: Optional[str] = Query(None, description="First financial period to"),
    fiscal_year_from: Optional[str] = Query(None, description="Fiscal year from"),
    fiscal_year_to: Optional[str] = Query(None, description="Fiscal year to"),
    declared_email: Optional[str] = Query(None, description="Declared email"),
    jurisdiction: Optional[str] = Query(None, description="Jurisdiction"),
    q: Optional[str] = Query(None, description="Search across main text fields (legal_name, address, trade_name, social_object)"),
):
    """
    Search entities by any combination of column filters, with pagination.

    All filter parameters are optional. String filters use case-insensitive partial match.
    Returns one entity per registration_id with merged data from all deposit versions:
    each field takes its value from the most recent deposit that contains it
    (e.g. social_object from L160092539 overwrites L150172349 if both exist).
    Returns one page of results plus total count and pagination metadata.
    """
    try:
        params = {
            "registration_id": registration_id,
            "company_status": company_status,
            "legal_name": legal_name,
            "legal_name_abbreviation": legal_name_abbreviation,
            "legal_form": legal_form,
            "additional_mention": additional_mention,
            "trade_name": trade_name,
            "trade_name_abbreviation": trade_name_abbreviation,
            "address": address,
            "social_object": social_object,
            "type_of_capital": type_of_capital,
            "share_capital": share_capital,
            "denomination": denomination,
            "contribution_type": contribution_type,
            "contribution_percentage": contribution_percentage,
            "incorporation_date": incorporation_date,
            "duration": duration,
            "duration_end_date": duration_end_date,
            "first_financial_period_from": first_financial_period_from,
            "first_financial_period_to": first_financial_period_to,
            "fiscal_year_from": fiscal_year_from,
            "fiscal_year_to": fiscal_year_to,
            "declared_email": declared_email,
            "jurisdiction": jurisdiction,
        }
        db = get_database()
        entities_collection = db["entities"]

        skip = (page - 1) * page_size
        group_stage = {
            "$group": {
                "_id": "$registration_id",
                "doc_created_at": {"$first": "$created_at"},
                "doc_company_status": {"$first": "$company_status"},
                "versions": {"$push": {"k": "$dataArr.k", "v": "$dataArr.v"}},
            }
        }
        base_pipeline = _build_entity_search_query(params, q)
        count_pipeline = base_pipeline + [group_stage, {"$count": "total"}]
        data_pipeline = base_pipeline + [
            group_stage,
            {"$sort": {"_id": 1}},
            {"$skip": skip},
            {"$limit": page_size},
        ]

        count_cursor = entities_collection.aggregate(count_pipeline)
        count_result = list(count_cursor)
        total = count_result[0]["total"] if count_result else 0

        data_cursor = entities_collection.aggregate(data_pipeline)
        raw_items = list(data_cursor)

        all_entities_list = []
        for item in raw_items:
            entity_copy = _merge_entity_versions(item.get("versions") or [])
            entity_copy["rcs_number"] = item.get("_id")
            entity_copy.pop("deposit_number", None)
            entity_copy.pop("filing_id", None)
            entity_copy.pop("status", None)
            entity_copy.pop("description", None)
            if item.get("doc_company_status") is not None:
                entity_copy["company_status"] = item["doc_company_status"]
            if item.get("doc_created_at"):
                if "created_at" not in entity_copy or not entity_copy["created_at"]:
                    entity_copy["created_at"] = item["doc_created_at"]
            entity_copy = serialize_for_api(entity_copy)
            all_entities_list.append(entity_copy)

        total_pages = math.ceil(total / page_size) if page_size else 0

        return {
            "items": all_entities_list,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed during entity search: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error searching entities: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search entities: {str(e)}",
        )


@router.get("/entities/deposits/search", summary="Search all entity deposits with pagination")
async def search_entity_deposits(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    registration_id: Optional[str] = Query(None, description="Filter by RCS / registration ID"),
    company_status: Optional[str] = Query(None, description="Filter by company status"),
    legal_name: Optional[str] = Query(None, description="Filter by legal name"),
    legal_name_abbreviation: Optional[str] = Query(None, description="Legal name abbreviation"),
    legal_form: Optional[str] = Query(None, description="Legal form"),
    additional_mention: Optional[str] = Query(None, description="Additional mention"),
    trade_name: Optional[str] = Query(None, description="Trade name"),
    trade_name_abbreviation: Optional[str] = Query(None, description="Trade name abbreviation"),
    address: Optional[str] = Query(None, description="Address"),
    social_object: Optional[str] = Query(None, description="Social object"),
    type_of_capital: Optional[str] = Query(None, description="Type of capital"),
    share_capital: Optional[str] = Query(None, description="Share capital"),
    denomination: Optional[str] = Query(None, description="Denomination"),
    contribution_type: Optional[str] = Query(None, description="Contribution type"),
    contribution_percentage: Optional[str] = Query(None, description="Contribution percentage"),
    incorporation_date: Optional[str] = Query(None, description="Incorporation date"),
    duration: Optional[str] = Query(None, description="Duration"),
    duration_end_date: Optional[str] = Query(None, description="Duration end date"),
    first_financial_period_from: Optional[str] = Query(None, description="First financial period from"),
    first_financial_period_to: Optional[str] = Query(None, description="First financial period to"),
    fiscal_year_from: Optional[str] = Query(None, description="Fiscal year from"),
    fiscal_year_to: Optional[str] = Query(None, description="Fiscal year to"),
    declared_email: Optional[str] = Query(None, description="Declared email"),
    jurisdiction: Optional[str] = Query(None, description="Jurisdiction"),
    q: Optional[str] = Query(None, description="Search across main text fields (legal_name, address, trade_name, social_object)"),
):
    """
    Search and paginate over ALL entity deposit records (raw data).

    Returns one item per deposit_number per registration_id. No merging: each historical
    deposit/filing is a separate row. Use this when you need full deposit-level data with
    search and pagination. String filters use case-insensitive partial match.
    """
    try:
        params = {
            "registration_id": registration_id,
            "company_status": company_status,
            "legal_name": legal_name,
            "legal_name_abbreviation": legal_name_abbreviation,
            "legal_form": legal_form,
            "additional_mention": additional_mention,
            "trade_name": trade_name,
            "trade_name_abbreviation": trade_name_abbreviation,
            "address": address,
            "social_object": social_object,
            "type_of_capital": type_of_capital,
            "share_capital": share_capital,
            "denomination": denomination,
            "contribution_type": contribution_type,
            "contribution_percentage": contribution_percentage,
            "incorporation_date": incorporation_date,
            "duration": duration,
            "duration_end_date": duration_end_date,
            "first_financial_period_from": first_financial_period_from,
            "first_financial_period_to": first_financial_period_to,
            "fiscal_year_from": fiscal_year_from,
            "fiscal_year_to": fiscal_year_to,
            "declared_email": declared_email,
            "jurisdiction": jurisdiction,
        }
        db = get_database()
        entities_collection = db["entities"]

        skip = (page - 1) * page_size
        base_pipeline = _build_entity_search_query(params, q)

        count_pipeline = base_pipeline + [{"$count": "total"}]
        data_pipeline = base_pipeline + [
            {"$sort": {"registration_id": 1, "dataArr.k": 1}},
            {"$skip": skip},
            {"$limit": page_size},
        ]

        count_cursor = entities_collection.aggregate(count_pipeline)
        count_result = list(count_cursor)
        total = count_result[0]["total"] if count_result else 0

        data_cursor = entities_collection.aggregate(data_pipeline)
        raw_items = list(data_cursor)

        all_entities_list = []
        for item in raw_items:
            entity_record = item.get("dataArr", {}).get("v") or {}
            if not isinstance(entity_record, dict):
                continue
            entity_copy = dict(entity_record)
            entity_copy["rcs_number"] = item.get("registration_id")
            entity_copy["deposit_number"] = item.get("dataArr", {}).get("k")
            entity_copy.pop("filing_id", None)
            entity_copy.pop("status", None)
            entity_copy.pop("description", None)
            if item.get("company_status") is not None:
                entity_copy["company_status"] = item["company_status"]
            doc_created_at = item.get("created_at")
            if doc_created_at and ("created_at" not in entity_copy or not entity_copy["created_at"]):
                entity_copy["created_at"] = doc_created_at
            entity_copy = serialize_for_api(entity_copy)
            all_entities_list.append(entity_copy)

        total_pages = math.ceil(total / page_size) if page_size else 0

        return {
            "items": all_entities_list,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed during deposit search: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error searching entity deposits: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search entity deposits: {str(e)}",
        )


@router.get("/all/entities", summary="Get all entity data from all RCS numbers")
async def get_all_entities_data():
    """
    Get all entity data from all RCS numbers.

    Returns all entity records from all RCS numbers, with each deposit number as a separate entity.
    Each entity will include its RCS number for identification.
    """
    try:
        db = get_database()
        entities_collection = db["entities"]

        all_docs = entities_collection.find({})

        all_entities_list = []

        for entity_doc in all_docs:
            rcs_number = entity_doc.get("registration_id")
            if not rcs_number:
                continue
            doc_created_at = serialize_for_api(entity_doc.get("created_at"))
            entities_data = entity_doc.get("data", {})

            if not isinstance(entities_data, dict):
                continue

            for deposit_number, entity_record in entities_data.items():
                if not isinstance(entity_record, dict):
                    continue
                entity_copy = dict(entity_record)
                entity_copy["rcs_number"] = rcs_number
                entity_copy["deposit_number"] = deposit_number
                entity_copy.pop("filing_id", None)
                entity_copy.pop("status", None)
                entity_copy.pop("description", None)
                if doc_created_at and ("created_at" not in entity_copy or not entity_copy["created_at"]):
                    entity_copy["created_at"] = doc_created_at
                entity_copy = serialize_for_api(entity_copy)

                all_entities_list.append(entity_copy)
        all_entities_list.sort(
            key=lambda x: (
                x.get("rcs_number", ""),
                x.get("filing_date") or x.get("last_scan") or "",
            ),
            reverse=False,
        )

        logger.info(f"Retrieved {len(all_entities_list)} total entity records from all RCS numbers")

        return {
            "rcs_number": "ALL",
            "legal_name": None,
            "status": None,
            "last_scan": None,
            "entities": all_entities_list,
            "total": len(all_entities_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving all entity data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve all entity data: {str(e)}",
        )


@router.get("/{rcs_number}/entities", summary="Get entity data for an RCS number")
async def get_entity_data(rcs_number: str):
    """
    Get entity data for a given RCS number (includes parent and child numbers).

    Returns entity data from the entities collection, organized by deposit number.
    Structure: ONE document per RCS, entity records nested under data[deposit_number]
    When a parent RCS number is provided, also returns child entities (e.g., B215183-1, B215183_A).
    """
    if rcs_number.lower() == "all":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Use /api/v1/view/all/entities endpoint to get all entities",
        )

    try:
        db = get_database()
        entities_collection = db["entities"]

        parent_pattern = re.escape(rcs_number)
        regex_pattern = f"^{parent_pattern}([-_].*)?$"

        entity_docs = list(entities_collection.find({"registration_id": {"$regex": regex_pattern}}))

        if not entity_docs:
            logger.info(f"No entity document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "company_status": None,
                "last_scan": None,
                "entities": [],
                "total": 0,
                "message": f"No entity data found for RCS {rcs_number}",
            }

        entity_doc = None
        for doc in entity_docs:
            if doc.get("registration_id") == rcs_number:
                entity_doc = doc
                break
        if not entity_doc:
            entity_doc = entity_docs[0]

        all_entities_data = {}
        for doc in entity_docs:
            doc_data = doc.get("data", {})
            if isinstance(doc_data, dict):
                for deposit_num, entity_record in doc_data.items():
                    if deposit_num not in all_entities_data:
                        all_entities_data[deposit_num] = entity_record
                    else:
                        existing = all_entities_data[deposit_num]
                        existing_has_name = existing.get("legal_name")
                        new_has_name = (
                            entity_record.get("legal_name") if isinstance(entity_record, dict) else False
                        )

                        if new_has_name and not existing_has_name:
                            all_entities_data[deposit_num] = entity_record
                        elif not new_has_name and existing_has_name:
                            pass
                        else:
                            if isinstance(entity_record, dict):
                                merged = dict(existing)
                                for key, value in entity_record.items():
                                    if value is not None or key not in merged:
                                        merged[key] = value
                                all_entities_data[deposit_num] = merged

        if not entity_doc:
            logger.info(f"No entity document found for RCS {rcs_number}")
            return {
                "rcs_number": rcs_number,
                "legal_name": None,
                "status": None,
                "company_status": None,
                "last_scan": None,
                "entities": [],
                "total": 0,
                "message": f"No entity data found for RCS {rcs_number}",
            }
        entities_data = all_entities_data

        if not isinstance(entities_data, dict):
            logger.warning(f"Invalid data structure for RCS {rcs_number}: data field is not a dict")
            entities_data = {}

        entities_list = []
        for deposit_number, entity_record in entities_data.items():
            if not isinstance(entity_record, dict):
                logger.warning(f"Invalid entity record for deposit {deposit_number} in RCS {rcs_number}")
                continue
            entity_copy = dict(entity_record)
            entity_copy["deposit_number"] = deposit_number
            entity_copy.pop("filing_id", None)
            entity_copy.pop("status", None)
            entity_copy.pop("description", None)
            entity_copy = serialize_for_api(entity_copy)
            entities_list.append((deposit_number, entity_copy))
        entities_list.sort(
            key=lambda x: extract_number_from_deposit_number(x[0]) or 999999999,
            reverse=False,
        )
        entities_list = [e for _, e in entities_list]
        last_scan = serialize_for_api(entity_doc.get("last_scan"))
        created_at = serialize_for_api(entity_doc.get("created_at"))
        updated_at = serialize_for_api(entity_doc.get("updated_at"))

        if created_at:
            for entity in entities_list:
                if "created_at" not in entity or not entity["created_at"]:
                    entity["created_at"] = created_at

        top_level_company_status = entity_doc.get("company_status")
        if top_level_company_status is not None:
            for entity in entities_list:
                entity["company_status"] = top_level_company_status

        logger.info(f"Retrieved {len(entities_list)} entity records for RCS {rcs_number}")

        legal_name = entity_doc.get("legal_name")
        if not legal_name and entities_list:
            for entity in entities_list:
                if entity.get("legal_name"):
                    legal_name = entity.get("legal_name")
                    break

        return {
            "rcs_number": rcs_number,
            "legal_name": legal_name,
            "status": entity_doc.get("status"),
            "company_status": entity_doc.get("company_status"),
            "last_scan": last_scan,
            "created_at": created_at,
            "updated_at": updated_at,
            "entities": entities_list,
            "total": len(entities_list),
        }

    except OperationFailure as e:
        logger.error(f"MongoDB operation failed for RCS {rcs_number}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database operation failed: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error retrieving entity data for RCS {rcs_number}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve entity data: {str(e)}",
        )
