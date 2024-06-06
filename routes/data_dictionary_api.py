import codecs
import csv

from fastapi import APIRouter, UploadFile

from database import database
from models.models import DataDictionaries, DataDictionaryTerms, AccessCredentials
from serializers.data_dictionary_serializer import data_dictionary_list_entity, data_dictionary_entity, \
    data_dictionary_terms_list_entity

router = APIRouter()


@router.get("/data_dictionaries")
async def data_dictionaries():
    dictionaries = DataDictionaries.objects().all()
    dictionary_data = []

    for dictionary in dictionaries:
        terms = DataDictionaryTerms.objects.filter(dictionary_id=dictionary.id)
        data_source = AccessCredentials.objects.filter(id=dictionary.datasource_id)
        response_terms = data_dictionary_terms_list_entity(terms)
        dictionary_data.append({"name": dictionary.name, "data_source": dictionary.name, "dictionary_terms": response_terms})

    return dictionary_data


def add_data_dictionary():
    return {}


@router.post("/create_data_dictionary")
async def create_data_dictionary(
        dictionary_name: str,
        is_published: bool = False,
        datasource_id: str = None
):
    # Create a new data dictionary object
    dictionary = DataDictionaries(
        name=dictionary_name,
        is_published=is_published,
        datasource_id=datasource_id
    )
    dictionary.save()

    return {"message": "Data dictionary created successfully"}


@router.post("/upload_data_dictionary_terms/{dictionary_id}")
async def upload_data_dictionary_terms(
        dictionary_id: str,
        file: UploadFile,
):
    # Retrieve the data dictionary object
    dictionary = DataDictionaries.objects.get(id=dictionary_id)

    # Create a list to store the data dictionary terms
    terms = []

    with file.file as csvfile:  # Open the file in text mode
        # reader = csv.reader(csvfile)
        reader = csv.reader(codecs.iterdecode(csvfile, 'utf-8'))
        header = next(reader)  # skip the header row
        print(header)
        # TODO: Column header order sensitivity
        for row in reader:
            term = row[0]
            data_type = row[1]
            is_required = bool(row[2])
            term_description = row[3] or None
            expected_values = row[4] or None
            term_obj = DataDictionaryTerms(
                dictionary_id=dictionary.id,
                term=term,
                data_type=data_type,
                is_required=is_required,
                term_description=term_description,
                expected_values=expected_values
            )
            # Save the data dictionary terms to the database
            term_obj.save()
    return {"message": "Data dictionary terms uploaded successfully"}
