from collections import defaultdict
from cassandra.cqlengine.query import DoesNotExist

from fastapi import APIRouter, UploadFile
from pydantic import BaseModel, Field

from database import database
from models.models import DataDictionaries, DataDictionaryTerms, DataDictionariesUSL, DataDictionaryTermsUSL
from serializers.data_dictionary_serializer import data_dictionary_list_entity, data_dictionary_entity, \
    data_dictionary_terms_list_entity, data_dictionary_term_entity, data_dictionary_usl_list_entity

router = APIRouter()


@router.get("/data_dictionary_terms_usl")
async def data_dictionary_terms_usl():

    terms = DataDictionaryTermsUSL.objects.all()
    response_terms = data_dictionary_terms_list_entity(terms)
    grouped_terms = defaultdict(list)
    for term in response_terms:
        grouped_terms[term['dictionary']].append(term)
    # dictionary_data.append({"name": dictionary.name, "dictionary_terms": response_terms})
    formatted_terms = [{"name": dictionary_name, "dictionary_terms": terms} for dictionary_name, terms in
                       grouped_terms.items()]
    return formatted_terms


@router.get("/data_dictionaries_usl")
async def data_dictionaries_usl():
    dictionaries = DataDictionariesUSL.objects().all()

    response_terms = data_dictionary_usl_list_entity(dictionaries)
    return response_terms


@router.get("/data_dictionary_terms")
async def data_dictionary_terms():

    terms = DataDictionaryTerms.objects.all()
    response_terms = data_dictionary_terms_list_entity(terms)
    grouped_terms = defaultdict(list)
    for term in response_terms:
        grouped_terms[term['dictionary']].append(term)
    # dictionary_data.append({"name": dictionary.name, "dictionary_terms": response_terms})
    formatted_terms = [{"name": dictionary_name, "dictionary_terms": terms} for dictionary_name, terms in
                       grouped_terms.items()]
    return formatted_terms


@router.get("/data_dictionary_terms/{dictionary_id}")
async def data_dictionary_terms(dictionary_id: str):
    try:
        terms = DataDictionaryTerms.objects.filter(dictionary_id=dictionary_id).allow_filtering().all()

        response_terms = data_dictionary_terms_list_entity(terms)
        if not response_terms:
            return {"name": None, "dictionary_terms": []}

        grouped_terms = defaultdict(list)
        for term in response_terms:
            grouped_terms[term['dictionary']].append(term)

        formatted_terms = [{"name": dictionary_name, "dictionary_terms": terms} for dictionary_name, terms in
                           grouped_terms.items()]
        return formatted_terms[0]

    except DoesNotExist:
        return {"name": None, "dictionary_terms": []}


@router.get("/data_dictionaries")
async def data_dictionaries():
    dictionaries = DataDictionaries.objects().all()

    response_terms = data_dictionary_usl_list_entity(dictionaries)
    return response_terms


class SaveUSLDataDictionary(BaseModel):
    name: str = Field(..., description="")


@router.post("/create_data_dictionary_usl")
async def create_data_dictionary(
        data: SaveUSLDataDictionary
):
    # Create a new data dictionary object
    dictionary = DataDictionariesUSL(
        name=data.name
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
    data_dictionary = DataDictionariesUSL.objects.filter(id=data.dictionary).allow_filtering().first()

    for row in data.data:
        term = row['column']
        data_type = row['data_type']
        is_required = bool(row['is_required'])
        term_description = row['description'] or None
        expected_values = row['expected_values'] or None

        # Check if the term already exists
        term_obj = DataDictionaryTermsUSL.objects.filter(dictionary_id=data.dictionary, term=term).allow_filtering().first()

        if term_obj:
            # If the term exists, update it
            term_obj.data_type = data_type
            term_obj.is_required = is_required
            term_obj.term_description = term_description
            term_obj.expected_values = expected_values
            term_obj.save()
        else:
            # If the term doesn't exist, create a new one
            term_obj = DataDictionaryTermsUSL(
                dictionary_id=data.dictionary,
                dictionary=data_dictionary.name,
                term=term,
                data_type=data_type,
                is_required=is_required,
                term_description=term_description,
                expected_values=expected_values
            )
            term_obj.save()

        # Save the data dictionary terms to the database
        term_obj.save()
    return {"message": "Data dictionary terms uploaded successfully"}


def sync_dictionaries(datasource_id: str) -> dict:
    usl_dicts = DataDictionariesUSL.objects().all()
    dict_id_map = {}

    for usl_dict in usl_dicts:
        existing_dict = DataDictionaries.objects().filter(name=usl_dict.name).allow_filtering().first()
        if not existing_dict:
            new_dict = DataDictionaries(
                name=usl_dict.name,
                is_published=usl_dict.is_published,
                datasource_id=datasource_id
            )
            new_dict.save()
            dict_id_map[usl_dict.name] = new_dict.id
        else:
            existing_dict.is_published = usl_dict.is_published
            existing_dict.save()
            dict_id_map[usl_dict.name] = existing_dict.id

    return dict_id_map


def sync_terms(dict_id_map: dict):
    usl_terms = DataDictionaryTermsUSL.objects().all()

    for usl_term in usl_terms:
        dictionary_id = dict_id_map.get(usl_term.dictionary)
        if dictionary_id:
            existing_term = DataDictionaryTerms.objects().filter(dictionary=usl_term.dictionary, term=usl_term.term).allow_filtering().first()
            if not existing_term:
                new_term = DataDictionaryTerms(
                    dictionary=usl_term.dictionary,
                    dictionary_id=dictionary_id,
                    term=usl_term.term,
                    data_type=usl_term.data_type,
                    is_required=usl_term.is_required,
                    term_description=usl_term.term_description,
                    expected_values=usl_term.expected_values,
                    is_active=usl_term.is_active
                )
                new_term.save()
            else:
                existing_term.data_type = usl_term.data_type
                existing_term.is_required = usl_term.is_required
                existing_term.term_description = usl_term.term_description
                existing_term.expected_values = usl_term.expected_values
                existing_term.is_active = usl_term.is_active
                existing_term.save()

    return {"message": "Data dictionary terms synced successfully"}


@router.get("/sync_all/{datasource_id}")
def sync_all(datasource_id: str):
    dict_id_map = sync_dictionaries(datasource_id)
    sync_terms(dict_id_map)
    return {"message": "All data synced successfully"}
