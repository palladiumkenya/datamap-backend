import uuid
from collections import defaultdict
from uuid import UUID

from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.query import DoesNotExist
from cassandra.cqlengine import columns, models

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from database import database
from models.models import DataDictionaries, DataDictionaryTerms
from models.usl_models import DataDictionariesUSL, DataDictionaryTermsUSL
from serializers.data_dictionary_serializer import data_dictionary_terms_list_entity, data_dictionary_usl_list_entity

router = APIRouter()


# Datamap dictionary management apis
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
async def data_dictionary_term(dictionary_id: str):
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


def sync_dictionaries(datasource_id: str) -> dict:
    usl_dicts = DataDictionariesUSL.objects().all()
    dict_id_map = {}
    active_dicts = set()

    for usl_dict in usl_dicts:
        active_dicts.add(usl_dict.name)
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
    # Deactivate dictionaries that are no longer present in usl_dicts
    existing_dicts = DataDictionaries.objects().filter(datasource_id=datasource_id).allow_filtering()
    for existing_dict in existing_dicts:
        if existing_dict.name not in active_dicts:
            DataDictionaries.objects(id=existing_dict.id).first().delete()

    return dict_id_map


def sync_terms(dict_id_map: dict):
    usl_terms = DataDictionaryTermsUSL.objects().all()
    active_terms = set()

    for usl_term in usl_terms:
        dictionary_id = dict_id_map.get(usl_term.dictionary)
        if dictionary_id:
            active_terms.add((usl_term.dictionary, usl_term.term))
            existing_term = DataDictionaryTerms.objects().filter(dictionary=usl_term.dictionary,
                                                                 term=usl_term.term).allow_filtering().first()
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

    # Deactivate terms that are no longer present in usl_terms
    existing_terms = DataDictionaryTerms.objects().filter(dictionary_id__in=list(dict_id_map.values())).allow_filtering()
    for existing_term in existing_terms:
        if (existing_term.dictionary, existing_term.term) not in active_terms:
            DataDictionaryTerms.objects(id=existing_term.id).first().delete()
    return {"message": "Data dictionary terms synced successfully"}


# Function to map SQL data types to Cassandra columns
def get_cassandra_column(data_type):
    """
    Maps SQL data types to corresponding Cassandra columns.
    :param data_type: SQL data type.
    :return: cassandra.cqlengine.columns.Column: Corresponding Cassandra column type.
    """
    if str(data_type).upper() in ["DATE", "DATETIME", "DATETIME2"]:
        return columns.DateTime
    elif str(data_type).upper() in ["NVARCHAR", "VARCHAR", "TEXT"]:
        return columns.Text
    elif str(data_type).upper() in ["INT", "INTEGER", "BIGINT", "NUMERIC"]:
        return columns.Integer
    elif str(data_type).upper() == "BOOLEAN":
        return columns.Boolean
    elif str(data_type).upper() == "FLOAT":
        return columns.Float
    elif str(data_type).upper() == "DOUBLE":
        return columns.Double
    elif str(data_type).upper() == "UUID":
        return columns.UUID
    else:
        # Default to Text if data type not recognized
        return columns.Text


# Function to create Cassandra tables based on data dictionary terms
def create_tables():
    """
    Creates Cassandra tables based on data dictionary terms.
    :return: None
    """
    terms = DataDictionaryTerms.objects().all()
    table_columns = {}

    # Iterate over terms to create table structures
    for term in terms:
        table_name = term.dictionary.lower()
        column_name = term.term.lower()
        column_type = get_cassandra_column(term.data_type)
        column_required = term.is_required

        if table_name not in table_columns:
            table_columns[table_name] = {}
        # Add column to table_columns dictionary
        table_columns[table_name][column_name] = column_type(required=column_required)

    # Create tables and synchronize with Cassandra
    for table_name, tbl_columns in table_columns.items():
        # Add primary key column to each table
        tbl_columns[f'{table_name}_id'] = columns.UUID(primary_key=True, default=uuid.uuid1)
        # Create dynamic table class and synchronize with Cassandra
        dynamic_table = type(table_name, (models.Model,), tbl_columns)
        dynamic_table.__keyspace__ = database.KEYSPACE
        try:
            drop_table(dynamic_table)
        except:
            pass
        sync_table(dynamic_table)


@router.get("/sync_all/{datasource_id}")
def sync_all(datasource_id: str, background_tasks: BackgroundTasks):
    dict_id_map = sync_dictionaries(datasource_id)
    sync_terms(dict_id_map)
    background_tasks.add_task(create_tables)
    return {"message": "All data synced successfully"}
