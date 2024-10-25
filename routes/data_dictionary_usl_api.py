from collections import defaultdict
from uuid import UUID

from cassandra.cqlengine.query import DoesNotExist

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from models.usl_models import DataDictionariesUSL, DataDictionaryTermsUSL
from serializers.data_dictionary_serializer import data_dictionary_terms_list_entity, data_dictionary_usl_list_entity

router = APIRouter()


# USL dictionary management apis
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


@router.get("/data_dictionary_terms_usl/{dictionary_id}")
async def data_dictionary_term_usl(dictionary_id: str):
    try:
        terms = DataDictionaryTermsUSL.objects.filter(dictionary_id=dictionary_id).allow_filtering().all()

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
    data_dictionary = (DataDictionariesUSL
                       .objects
                       .filter(id=data.dictionary)
                       .allow_filtering()
                       .all())
    if not data_dictionary:
        raise HTTPException(status_code=404, detail="Dictionary not found")
    latest_dictionary = max(data_dictionary, key=lambda dictionary: dictionary.version_number)
    terms_count = DataDictionaryTermsUSL.objects.filter(dictionary_id=data.dictionary).count()

    print(terms_count)
    if terms_count == 0:
        add_dict_terms(data, latest_dictionary)
    else:
        new_data_dictionary = DataDictionariesUSL(
            name=latest_dictionary.name,
            version_number=(latest_dictionary.version_number +1)
        )
        new_data_dictionary.save()
        add_dict_terms(data, new_data_dictionary)
    return {"message": "Data dictionary terms uploaded successfully"}


def add_dict_terms(data, data_dictionary):
    for row in data.data:
        term = row['column']
        data_type = row['data_type']
        is_required = bool(row['is_required'])
        term_description = row['description'] or None
        expected_values = row['expected_values'] or None

        # Check if the term already exists
        # term_obj = DataDictionaryTermsUSL.objects.filter(dictionary_id=data.dictionary,
        #                                                  term=term).allow_filtering().first()
        #
        # if term_obj:
        #     # If the term exists, update it
        #     term_obj.data_type = data_type
        #     term_obj.is_required = is_required
        #     term_obj.term_description = term_description
        #     term_obj.expected_values = expected_values
        #     term_obj.save()
        # else:
        # If the term doesn't exist, create a new one
        term_obj = DataDictionaryTermsUSL(
            dictionary_id=str(data_dictionary.id),
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


class DataDictionaryTermsUSLUpdate(BaseModel):
    data_type: str = None
    is_required: bool = None
    term_description: str = None
    expected_values: str = None
    is_active: bool = None


@router.put("/update_data_dictionary_terms_usl/{term_id}")
def update_data_dictionary_term_usl(term_id: str, data: DataDictionaryTermsUSLUpdate):
    term = DataDictionaryTermsUSL.objects(id=UUID(term_id)).first()
    if not term:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    if data.data_type is not None:
        term.data_type = data.data_type
    if data.is_required is not None:
        term.is_required = data.is_required
    if data.term_description is not None:
        term.term_description = data.term_description
    if data.expected_values is not None:
        term.expected_values = data.expected_values
    term.save()
    return term


@router.delete("/delete_data_dictionary_terms_usl/{term_id}")
def delete_data_dictionary_term_usl(term_id: str):
    term = DataDictionaryTermsUSL.objects(id=UUID(term_id)).first()
    if not term:
        raise HTTPException(status_code=404, detail="Data dictionary term not found")

    term.delete()
    return {"message": "Data dictionary term deleted successfully"}
