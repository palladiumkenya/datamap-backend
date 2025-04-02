import copy
import json
import re
import secrets
import time

import jwt
import exrex
from collections import defaultdict
from datetime import datetime
from uuid import UUID

from cassandra.cqlengine.query import DoesNotExist

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database.database import get_db
from models.usl_models import DataDictionariesUSL, DataDictionaryTermsUSL, DictionaryChangeLog, \
    UniversalDictionaryTokens, UniversalDictionaryFacilityPulls
from serializers.data_dictionary_serializer import data_dictionary_terms_list_entity, data_dictionary_usl_list_entity, \
    data_dictionary_change_log_entity, data_dictionary_term_entity, data_dictionary_usl_entity
from serializers.universal_dictionary_serializer import universal_dictionary_facility_pulls_serializer_list

router = APIRouter()


# USL dictionary management apis
@router.get("/data_dictionary_terms_usl")
async def data_dictionary_terms_usl(db: Session = Depends(get_db)):
    terms = db.query(DataDictionaryTermsUSL).all()
    response_terms = data_dictionary_terms_list_entity(terms)
    grouped_terms = defaultdict(list)
    for term in response_terms:
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
async def data_dictionaries_usl(db: Session = Depends(get_db)):
    dictionaries = db.query(DataDictionariesUSL).all()

    response_terms = data_dictionary_usl_list_entity(dictionaries)
    return response_terms


@router.get("/data_dictionary_terms_usl/{dictionary_id}")
async def data_dictionary_term_usl(dictionary_id: str, db: Session = Depends(get_db)):
    try:
        terms = db.query(DataDictionaryTermsUSL).filter(DataDictionaryTermsUSL.dictionary_id == dictionary_id).all()

        response_terms = data_dictionary_terms_list_entity(terms)
        if not response_terms:
            dictionary = db.query(DataDictionariesUSL).filter(DataDictionariesUSL.id == dictionary_id).first()
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
        data: SaveUSLDataDictionary, db: Session = Depends(get_db)
):
    # Create a new data dictionary object
    dictionary = DataDictionariesUSL(
        name=data.name.lower(),
        version_number=1
    )
    db.add(dictionary)
    db.commit()

    return {"message": "Data dictionary created successfully"}


class SaveDataDictionary(BaseModel):
    data: list = Field(..., description="")
    dictionary: str = Field(..., description="")


@router.post("/add_data_dictionary_terms")
async def add_data_dictionary_terms(
        data: SaveDataDictionary, db: Session = Depends(get_db)
):
    data_dictionary = db.query(DataDictionariesUSL).filter(DataDictionariesUSL.id == data.dictionary).first()
    if not data_dictionary:
        raise HTTPException(status_code=404, detail="Dictionary not found")
    # latest_dictionary = max(data_dictionary, key=lambda dictionary: dictionary.version_number)
    terms_count = db.query(DataDictionaryTermsUSL).filter(DataDictionaryTermsUSL.dictionary_id == data.dictionary).all()

    if len(terms_count) != 0:
        data_dictionary.version_number = data_dictionary.version_number + 1
        db.commit()
    add_dict_terms(data, data_dictionary, db)
    return {"message": "Data dictionary terms uploaded successfully"}


def add_dict_terms(data, data_dictionary, db):
    """Adds terms to data dictionary and logs changes"""
    for row in data.data:
        term = row['column']
        data_type = row['data_type']
        is_required = bool(row['is_required'])
        term_description = row['description'] or None
        expected_values = row['expected_values'] or None

        # Check if the term already exists
        existing_term = db.query(DataDictionaryTermsUSL).filter(
            DataDictionaryTermsUSL.dictionary_id == data.dictionary,
            DataDictionaryTermsUSL.term == term
        ).first()

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
                version_number=data_dictionary.version_number,
                db=db
            )
            # If the term exists, update it
            existing_term.data_type = data_type
            existing_term.is_required = is_required
            existing_term.term_description = term_description
            existing_term.expected_values = expected_values
            db.commit()
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
            db.add(new_term)
            db.commit()

            log_dictionary_change(
                dictionary_id=data_dictionary.id,
                term_id=new_term.id,
                operation="ADD",
                new_value=data_dictionary_term_entity(new_term),
                version_number=data_dictionary.version_number,
                db=db
            )


class DataDictionaryTermsUSLUpdate(BaseModel):
    data_type: str = None
    is_required: bool = None
    term_description: str = None
    expected_values: str = None
    is_active: bool = None


@router.put("/update_data_dictionary_terms_usl/{term_id}")
def update_data_dictionary_term_usl(term_id: str, data: DataDictionaryTermsUSLUpdate, db: Session = Depends(get_db)):
    # Fetch the term
    term = db.query(DataDictionaryTermsUSL).filter(DataDictionaryTermsUSL.id == UUID(term_id)).first()
    if not term:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    # Capture for logging
    old_term = copy.deepcopy(term)

    # update term attributes
    if data.data_type is not None:
        term.data_type = data.data_type
    if data.is_required is not None:
        term.is_required = data.is_required
    if data.term_description is not None:
        term.term_description = data.term_description
    if data.expected_values is not None:
        term.expected_values = data.expected_values
    db.commit()

    # Fetch dict for version update
    dictionary = db.query(DataDictionariesUSL).filter(DataDictionariesUSL.id == UUID(term.dictionary_id)).first()
    if dictionary:
        dictionary.version_number += 1
        db.commit()

        # log change
        log_dictionary_change(
            dictionary_id=dictionary.id,
            term_id=term.id,
            operation="EDIT",
            old_value=data_dictionary_term_entity(old_term),
            new_value=data_dictionary_term_entity(term),
            version_number=dictionary.version_number,
            db=db
        )

    return term


@router.delete("/delete_data_dictionary_terms_usl/{term_id}")
def delete_data_dictionary_term_usl(term_id: str, db: Session = Depends(get_db)):
    term = db.query(DataDictionaryTermsUSL).filter(DataDictionaryTermsUSL.id == UUID(term_id)).first()
    if not term:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    dictionary = db.query(DataDictionariesUSL).filter(DataDictionariesUSL.id == UUID(term.dictionary_id)).first()
    if dictionary:
        dictionary.version_number += 1

        # log change
        log_dictionary_change(
            dictionary_id=dictionary.id,
            term_id=term.id,
            operation="DELETE",
            old_value=data_dictionary_term_entity(term),
            version_number=dictionary.version_number,
            db=db
        )

    db.delete(term)
    db.commit()
    return {"message": "Data dictionary term deleted successfully"}


@router.delete("/delete_data_dictionary_usl/{dict_id}")
def delete_data_dictionary_usl(dict_id: str, db: Session = Depends(get_db)):
    dictionary = db.query(DataDictionariesUSL).filter(DataDictionariesUSL.id == UUID(dict_id)).first()
    if not dictionary:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    db.query(DataDictionaryTermsUSL).filter(DataDictionaryTermsUSL.dictionary_id == str(dictionary.id)).delete()

    db.delete(dictionary)
    db.commit()

    return {"message": "Data dictionary deleted successfully"}


def log_dictionary_change(dictionary_id, term_id, operation, db, old_value=None, new_value=None, version_number=None):
    print(old_value, new_value)

    change_log = DictionaryChangeLog(
        dictionary_id=dictionary_id,
        term_id=term_id,
        operation=operation,
        old_value=json.dumps(old_value, default=json_serializer) if old_value else None,
        new_value=json.dumps(new_value, default=json_serializer) if new_value else None,
        version_number=version_number,
    )
    db.add(change_log)
    db.commit()


def json_serializer(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} not serializable")


@router.get("/get_change_logs/{dictionary_id}")
async def get_change_logs(dictionary_id, db: Session = Depends(get_db)):
    try:
        logs = db.query(DictionaryChangeLog).filter(DictionaryChangeLog.dictionary_id == dictionary_id).all()
        if not logs:
            return []

        formatted_logs = {}
        for log in logs:
            version = f'Version {log.version_number}'
            if version not in formatted_logs:
                formatted_logs[version] = []
            formatted_logs[version].append(data_dictionary_change_log_entity(log))
        print(formatted_logs)
        return formatted_logs
    except DoesNotExist:
        raise HTTPException(status_code=404, detail="Dictionary does not exist")


@router.get("/get_universal_dictionaries")
def get_universal_dictionaries(db: Session = Depends(get_db)):
    try:
        universal_dictionaries = db.query(DataDictionariesUSL).filter(DataDictionariesUSL.is_published == True).all()

        grouped_terms = defaultdict(list)
        formatted_terms = []

        terms = db.query(DataDictionaryTermsUSL).all()

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
async def publish_universal_dictionary(data: PublishUniversalDictionary, db: Session = Depends(get_db)):
    dictionary = db.query(DataDictionariesUSL).filter(DataDictionariesUSL.id == data.id).first()
    if dictionary:
        dictionary.is_published = not dictionary.is_published
        db.commit()
        return dictionary
    else:
        raise HTTPException(status_code=404, detail="Dictionary Not Found")


@router.get("/universal_dictionary/token")
def get_universal_dictionary_token(db: Session = Depends(get_db)):
    token = db.query(UniversalDictionaryTokens).first()
    if token is not None:
        return {"token": token.universal_dictionary_token}
    else:
        payload = {
            "iss": "datamap.app",
            "sub": "universal_dictionary",
            "isBot": True,
            "tokenType": "BOT",
            "iat": int(time.time()),
            "exp": None
        }
        secret = secrets.token_hex(32)
        new_token = jwt.encode(payload, secret, algorithm="HS256")
        universal_token = UniversalDictionaryTokens(
            universal_dictionary_token=new_token,
            secret=secret
        )
        db.add(universal_token)
        db.commit()
        return {"token": new_token}


@router.post("/refresh_universal_dictionary/token")
async def refresh_universal_dictionary_token(db: Session = Depends(get_db)):
    payload = {
        "iss": "datamap.app",
        "sub": "universal_dictionary",
        "isBot": True,
        "tokenType": "BOT",
        "iat": int(time.time()),
        "exp": None
    }
    secret = secrets.token_hex(32)
    new_token = jwt.encode(payload, secret, algorithm="HS256")
    token = db.query(UniversalDictionaryTokens).first()

    if token is not None:
        token.universal_dictionary_token = new_token
        token.secret = secret
        db.commit()
        return {"token": new_token}
    else:
        raise HTTPException(status_code=500, detail="No token available")


@router.get('/get_facility_pulls')
async def get_facility_pulls(db: Session = Depends(get_db)):
    facility_pulls = db.query(UniversalDictionaryFacilityPulls).all()
    facility_pulls = universal_dictionary_facility_pulls_serializer_list(facility_pulls)
    return {"success": True, "data": facility_pulls}
