import json
import exrex
from collections import defaultdict
from datetime import datetime
from uuid import UUID

from cassandra.cqlengine.query import DoesNotExist

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from models.usl_models import DataDictionariesUSL, DataDictionaryTermsUSL, DictionaryChangeLog
from serializers.data_dictionary_serializer import data_dictionary_terms_list_entity, data_dictionary_usl_list_entity, \
    data_dictionary_change_log_entity, data_dictionary_term_entity, data_dictionary_usl_entity

router = APIRouter()


# USL dictionary management apis
@router.get("/data_dictionary_terms_usl")
async def data_dictionary_terms_usl():
    terms = DataDictionaryTermsUSL.objects.all()
    response_terms = data_dictionary_terms_list_entity(terms)
    grouped_terms = defaultdict(list)
    for term in response_terms:
        # term["values_examples"] = irregular_express(term["expected_values"])
        grouped_terms[term['dictionary']].append(term)
    # dictionary_data.append({"name": dictionary.name, "dictionary_terms": response_terms})
    formatted_terms = [{"name": dictionary_name, "dictionary_terms": terms} for dictionary_name, terms in
                       grouped_terms.items()]
    return formatted_terms


def irregular_express(pattern):
    """
    Converts a regular expression into a list of possible matches
    :param pattern: Regular expression to check
    :return: range_exp: List of possible matches
    """
    range_exp = exrex.getone(pattern)
    return range_exp


@router.get("/data_dictionaries_usl")
async def data_dictionaries_usl():
    dictionaries = DataDictionariesUSL.objects().all()

    response_terms = data_dictionary_usl_list_entity(dictionaries)
    return response_terms


@router.get("/data_dictionary_terms_usl/{dictionary_id}")
async def data_dictionary_term_usl(dictionary_id: str):
    try:
        terms = DataDictionaryTermsUSL.objects.filter(dictionary_id=dictionary_id).allow_filtering().all()

        response_terms = data_dictionary_terms_list_entity(terms)
        if not response_terms:
            dictionary = DataDictionariesUSL.objects.get(id=dictionary_id)
            dictionary_response = data_dictionary_usl_entity(dictionary)
            if dictionary_response:
                return {"name": dictionary_response["name"], "dictionary_terms": []}
            return {"name": None, "dictionary_terms": []}

        grouped_terms = defaultdict(list)
        for term in response_terms:
            grouped_terms[term['dictionary']].append(term)

        formatted_terms = [{"name": dictionary_name, "dictionary_terms": terms} for dictionary_name, terms in
                           grouped_terms.items()]
        return formatted_terms[0]

    except DoesNotExist:
        return {"name": None, "dictionary_terms": []}


class SaveUSLDataDictionary(BaseModel):
    name: str = Field(..., description="")


@router.post("/create_data_dictionary_usl")
async def create_data_dictionary(
        data: SaveUSLDataDictionary
):
    # Create a new data dictionary object
    dictionary = DataDictionariesUSL(
        name=data.name,
        version_number=1
    )
    dictionary.save()

    return {"message": "Data dictionary created successfully"}


class SaveDataDictionary(BaseModel):
    data: list = Field(..., description="")
    dictionary: str = Field(..., description="")


@router.post("/add_data_dictionary_terms")
async def add_data_dictionary_terms(
        data: SaveDataDictionary,
):
    data_dictionary = DataDictionariesUSL.objects(id=data.dictionary).first()
    if not data_dictionary:
        raise HTTPException(status_code=404, detail="Dictionary not found")
    # latest_dictionary = max(data_dictionary, key=lambda dictionary: dictionary.version_number)
    terms_count = DataDictionaryTermsUSL.objects.filter(dictionary_id=data.dictionary).count()

    if terms_count != 0:
        data_dictionary.update(version_number=data_dictionary.version_number + 1)
        data_dictionary.save()
    add_dict_terms(data, data_dictionary)
    return {"message": "Data dictionary terms uploaded successfully"}


def add_dict_terms(data, data_dictionary):
    """Adds terms to data dictionary and logs changes"""
    for row in data.data:
        term = row['column']
        data_type = row['data_type']
        is_required = bool(row['is_required'])
        term_description = row['description'] or None
        expected_values = row['expected_values'] or None

        # Check if the term already exists
        existing_term = DataDictionaryTermsUSL.objects.filter(dictionary_id=data.dictionary, term=term).allow_filtering().first()

        if existing_term:
            log_dictionary_change(
                dictionary_id=data_dictionary.id,
                term_id=existing_term.id,
                operation="EDIT",
                old_value=data_dictionary_term_entity(existing_term),
                new_value={
                    "data_type": data_type,
                    "is_required": is_required,
                    "expected_values": expected_values,
                    "term_description": term_description
                },
                version_number=data_dictionary.version_number
            )
            # If the term exists, update it
            existing_term.data_type = data_type
            existing_term.is_required = is_required
            existing_term.term_description = term_description
            existing_term.expected_values = expected_values
            existing_term.save()
        else:
            # If the term doesn't exist, create a new one
            new_term = DataDictionaryTermsUSL(
                dictionary_id=str(data_dictionary.id),
                dictionary=data_dictionary.name,
                term=term,
                data_type=data_type,
                is_required=is_required,
                term_description=term_description,
                expected_values=expected_values
            )
            new_term.save()

            log_dictionary_change(
                dictionary_id=data_dictionary.id,
                term_id=new_term.id,
                operation="ADD",
                new_value=data_dictionary_term_entity(new_term),
                version_number=data_dictionary.version_number
            )


class DataDictionaryTermsUSLUpdate(BaseModel):
    data_type: str = None
    is_required: bool = None
    term_description: str = None
    expected_values: str = None
    is_active: bool = None


@router.put("/update_data_dictionary_terms_usl/{term_id}")
def update_data_dictionary_term_usl(term_id: str, data: DataDictionaryTermsUSLUpdate):
    # Fetch the term
    term = DataDictionaryTermsUSL.objects(id=UUID(term_id)).first()
    if not term:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    # Capture for logging
    old_term = term

    # update term attributes
    if data.data_type is not None:
        term.data_type = data.data_type
    if data.is_required is not None:
        term.is_required = data.is_required
    if data.term_description is not None:
        term.term_description = data.term_description
    if data.expected_values is not None:
        term.expected_values = data.expected_values
    term.save()

    # Fetch dict for version update
    dictionary = DataDictionariesUSL.objects(id=UUID(term.dictionary_id)).first()
    if dictionary:
        dictionary.version_number += 1
        dictionary.save()

        # log change
        log_dictionary_change(
            dictionary_id=dictionary.id,
            term_id=term.id,
            operation="EDIT",
            old_value=data_dictionary_term_entity(old_term),
            new_value=data_dictionary_term_entity(term),
            version_number=dictionary.version_number
        )

    return term


@router.delete("/delete_data_dictionary_terms_usl/{term_id}")
def delete_data_dictionary_term_usl(term_id: str):
    term = DataDictionaryTermsUSL.objects(id=UUID(term_id)).first()
    if not term:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    dictionary = DataDictionariesUSL.objects(id=UUID(term.dictionary_id)).first()
    if dictionary:
        dictionary.version_number += 1
        dictionary.save()

        # log change
        log_dictionary_change(
            dictionary_id=dictionary.id,
            term_id=term.id,
            operation="DELETE",
            old_value=data_dictionary_term_entity(term),
            version_number=dictionary.version_number
        )

    term.delete()
    return {"message": "Data dictionary term deleted successfully"}


@router.delete("/delete_data_dictionary_usl/{dict_id}")
def delete_data_dictionary_usl(dict_id: str):
    dictionary = DataDictionariesUSL.objects(id=UUID(dict_id)).first()
    if not dictionary:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    terms = DataDictionaryTermsUSL.objects(dictionary_id=str(dictionary.id)).all()
    for term in terms:
        term.delete()
    dictionary.delete()

    return {"message": "Data dictionary deleted successfully"}


def log_dictionary_change(dictionary_id, term_id, operation, old_value=None, new_value=None, version_number=None):

    change_log = DictionaryChangeLog(
        dictionary_id=dictionary_id,
        term_id=term_id,
        operation=operation,
        old_value=json.dumps(old_value, default=json_serializer) if old_value else None,
        new_value=json.dumps(new_value, default=json_serializer) if new_value else None,
        version_number=version_number,
    )
    change_log.save()


def json_serializer(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} not serializable")


@router.get("/get_change_logs/{dictionary_id}")
async def get_change_logs(dictionary_id):
    try:
        logs = DictionaryChangeLog.objects.filter(dictionary_id=dictionary_id).allow_filtering().all()
        if not logs:
            return []

        formatted_logs = {}
        for log in logs:
            version = f'Version {log.version_number}'
            if version not in formatted_logs:
                formatted_logs[version] = []
            formatted_logs[version].append(data_dictionary_change_log_entity(log))
        return formatted_logs
    except DoesNotExist:
        raise HTTPException(status_code=404, detail="Dictionary does not exist")


@router.get("/get_universal_dictionaries")
def get_universal_dictionaries():
    try:
        universal_dictionaries = DataDictionariesUSL.objects.filter(is_published=True).allow_filtering().all()

        grouped_terms = defaultdict(list)
        formatted_terms = []

        terms = DataDictionaryTermsUSL.objects.all()

        response_terms = data_dictionary_terms_list_entity(terms)
        for term in response_terms:
            grouped_terms[term['dictionary']].append(term)

        # Format response for each universal dictionary
        for universal_dictionary in universal_dictionaries:
            universal_dictionary_data = data_dictionary_usl_entity(universal_dictionary)
            dict_terms = grouped_terms.get(universal_dictionary_data["name"], [])

            formatted_terms.append({"dictionary": universal_dictionary_data, "dictionary_terms": dict_terms})
        return {"data": formatted_terms, "detail": "Connection successful"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class PublishUniversalDictionary(BaseModel):
    id: str = Field(..., description="ID for dictionary to update")


@router.post("/publish/universal_dictionary")
async def publish_universal_dictionary(data: PublishUniversalDictionary):
    dictionary = DataDictionariesUSL.objects.filter(id=data.id).first()
    if dictionary:
        dictionary.is_published = not dictionary.is_published
        dictionary.save()
        return dictionary
    else:
        raise HTTPException(status_code=404, detail="Dictionary Not Found")
