import uuid
from collections import defaultdict

import requests
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from sqlalchemy import Column, UUID, DateTime, String, Integer, Boolean, Float, Double, MetaData, Table, inspect
from sqlalchemy.orm import Session

from database.database import get_db, engine
from models.models import DataDictionaries, DataDictionaryTerms, UniversalDictionaryConfig
from serializers.data_dictionary_serializer import data_dictionary_terms_list_entity, data_dictionary_usl_list_entity, \
    data_dictionary_entity

router = APIRouter()


# Datamap dictionary management apis
@router.get("/data_dictionary_terms")
async def data_dictionary_terms(db: Session = Depends(get_db)):
    terms = db.query(DataDictionaryTerms).all()
    response_terms = data_dictionary_terms_list_entity(terms)
    grouped_terms = defaultdict(list)
    for term in response_terms:
        grouped_terms[term['dictionary']].append(term)
    # dictionary_data.append({"name": dictionary.name, "dictionary_terms": response_terms})
    formatted_terms = [{"name": dictionary_name, "dictionary_terms": terms} for dictionary_name, terms in
                       grouped_terms.items()]
    return formatted_terms


@router.get("/data_dictionary_terms/{dictionary_id}")
async def data_dictionary_term(dictionary_id: str, db: Session = Depends(get_db)):
    try:
        terms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary_id == dictionary_id).all()

        response_terms = data_dictionary_terms_list_entity(terms)
        if not response_terms:
            return {"name": None, "dictionary_terms": []}

        grouped_terms = defaultdict(list)
        for term in response_terms:
            grouped_terms[term['dictionary']].append(term)

        formatted_terms = [{"name": dictionary_name, "dictionary_terms": terms} for dictionary_name, terms in
                           grouped_terms.items()]
        return formatted_terms[0]

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/data_dictionaries")
async def data_dictionaries(db: Session = Depends(get_db)):
    dictionaries = db.query(DataDictionaries).all()

    return data_dictionary_usl_list_entity(dictionaries)


def sync_dictionaries(datasource_id: str, usl_dicts: list, db) -> dict:
    dict_id_map = {}
    active_dicts = set()

    for usl_dict in usl_dicts:
        active_dicts.add(usl_dict['dictionary']['name'])
        dictionary = usl_dict['dictionary']

        existing_dict = db.query(DataDictionaries).filter(DataDictionaries.name == dictionary['name']).first()
        if not existing_dict:
            new_dict = DataDictionaries(name=dictionary['name'], is_published=dictionary['is_published'],
                                        version_number=dictionary['version_number'], datasource_id=datasource_id)
            db.add(new_dict)
            db.commit()
            print(new_dict.id)
            for term in usl_dict['dictionary_terms']:
                term['dictionary_id'] = new_dict.id

        else:
            existing_dict.is_published = dictionary['is_published']
            existing_dict.version_number = dictionary['version_number']
            db.commit()

            dict_id_map[dictionary['name']] = existing_dict.id
            for term in usl_dict['dictionary_terms']:
                term['dictionary_id'] = existing_dict.id
        sync_terms(usl_dict['dictionary_terms'], db)
    # Deactivate dictionaries that are no longer present in usl_dicts
    existing_dicts = db.query(DataDictionaries).all()
    for existing_dict in existing_dicts:
        if existing_dict.name not in active_dicts:
            db.query(DataDictionaries).filter(DataDictionaries.id == existing_dict.id).delete()
            db.commit()

    return dict_id_map


def sync_terms(terms, db):
    active_terms = set()
    dictionaries = []

    for usl_term in terms:
        dictionary_id = str(usl_term['dictionary_id'])
        dictionaries.append(dictionary_id)

        if dictionary_id:
            active_terms.add((usl_term['dictionary'], usl_term['term']))
            existing_term = db.query(DataDictionaryTerms).filter(
                DataDictionaryTerms.dictionary == usl_term['dictionary'],
                DataDictionaryTerms.term == usl_term['term']
            ).first()
            if not existing_term:
                new_term = DataDictionaryTerms(dictionary=usl_term['dictionary'], dictionary_id=dictionary_id,
                                               term=usl_term['term'], data_type=usl_term['data_type'],
                                               is_required=usl_term['is_required'],
                                               term_description=usl_term['term_description'],
                                               expected_values=usl_term['expected_values'],
                                               is_active=usl_term['is_active'])
                db.add(new_term)
                # new_term.save()
            else:
                existing_term.data_type = usl_term['data_type']
                existing_term.is_required = usl_term['is_required']
                existing_term.term_description = usl_term['term_description']
                existing_term.expected_values = usl_term['expected_values']
                existing_term.is_active = usl_term['is_active']
                db.add(existing_term)
            db.commit()

    # Deactivate terms that are no longer present in usl_terms
    existing_terms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary_id.in_(dictionaries)).all()
    for existing_term in existing_terms:
        if (existing_term.dictionary, existing_term.term) not in active_terms:
            is_deleted = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.id == existing_term.id).all()
            db.delete(is_deleted)
            db.commit()
    return {"message": "Data dictionary terms synced successfully"}


# Function to map SQL data types to Psql columns
def get_pgsql_column(data_type):
    """
    Maps SQL data types to corresponding PSQL columns.
    :param data_type: SQL data type.
    :return: sqlalchemy.Column: Corresponding PSQL column type.
    """
    if str(data_type).upper() in ["DATE", "DATETIME", "DATETIME2"]:
        return DateTime
    elif str(data_type).upper() in ["NVARCHAR", "VARCHAR", "TEXT"]:
        return String
    elif str(data_type).upper() in ["INT", "INTEGER", "BIGINT", "NUMERIC"]:
        return Integer
    elif str(data_type).upper() == "BOOLEAN":
        return Boolean
    elif str(data_type).upper() == "FLOAT":
        return Float
    elif str(data_type).upper() == "DOUBLE":
        return Double
    elif str(data_type).upper() == "UUID":
        return  UUID(as_uuid=True)
    else:
        # Default to Text if data type not recognized
        return String


# Function to create Postgresql tables based on data dictionary terms
def create_tables(db):
    """
    Creates PostgreSQL tables based on data dictionary terms.
    :return: None
    """
    terms = db.query(DataDictionaryTerms).all()
    table_columns = {}
    metadata = MetaData()

    # Iterate over terms to create table structures
    for term in terms:
        table_name = term.dictionary.lower()
        column_name = term.term.lower()
        column_type = get_pgsql_column(term.data_type)
        column_required = term.is_required

        if table_name not in table_columns:
            table_columns[table_name] = {}
        # Add column to table_columns dictionary
        table_columns[table_name][column_name] = Column(column_name, column_type, nullable=not column_required)

    # Create tables and synchronize with PSQL
    for table_name, tbl_columns in table_columns.items():
        # Add primary key column to each table
        tbl_columns[f'{table_name}_id'] = Column(f'{table_name}_id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
        columns_list = [column for column in tbl_columns.values()]
        # Create dynamic table class and synchronize with PSQL
        dynamic_table = Table(table_name, metadata, *columns_list)

        if inspect(engine).has_table(table_name):
            dynamic_table.drop(engine)
        metadata.create_all(engine)


def pull_dict_from_universal(universal_dict_config):
    """
    Pull data dictionary from universal dictionary via api
    :param universal_dict_config: api config for universal dictionary
    :return: data dictionary
    """
    headers = {"Authorization": f"Bearer {universal_dict_config.universal_dictionary_jwt}"}
    response = requests.get(universal_dict_config.universal_dictionary_url, headers=headers)

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.json())
    return response.json()


@router.get("/sync_all/{datasource_id}")
def sync_all(datasource_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    universal_dict_config = db.query(UniversalDictionaryConfig).first()
    if universal_dict_config is not None:
        response = pull_dict_from_universal(universal_dict_config)
        dict_map = sync_dictionaries(datasource_id, response.get("data"), db)
        background_tasks.add_task(create_tables, db)
        return {"message": "All data synced successfully", "data": dict_map}
    else:
        return {"message": "Please add a valid Universal Dictionary Configuration first"}


@router.get("/dictionary_version_notification")
def dictionary_version_notification(db: Session = Depends(get_db)):
    universal_dict_config = db.query(UniversalDictionaryConfig).first()
    if universal_dict_config is None:
        return {"message": "Please add a valid Universal Dictionary Configuration first"}

    response = pull_dict_from_universal(universal_dict_config)

    to_update = False
    to_update_count = 0
    existing_dictionaries = []
    for universal_dict in response.get("data"):
        dictionary = db.query(DataDictionaries).filter(DataDictionaries.name == universal_dict["dictionary"]["name"]).first()
        if not dictionary:
            continue
        existing_dictionaries.append(dictionary.id)
        if dictionary is not None:
            dictionary = data_dictionary_entity(dictionary)
            if (universal_dict["dictionary"]["version_number"] != dictionary["version_number"] or
                    universal_dict["dictionary"]["is_published"] != dictionary["is_published"]):
                to_update = True
                to_update_count += 1
        else:
            if universal_dict["dictionary"]["is_published"]:
                to_update = True
                to_update_count += 1

    # get changes to dictionaries that no longer exist
    deleted_dictionaries = db.query(DataDictionaries).all()
    if filtered_dicts := [
        obj
        for obj in deleted_dictionaries
        if obj.id not in existing_dictionaries
    ]:
        to_update = True
        to_update_count += len(filtered_dicts)

    return {
        "message": f"You have {to_update_count} pending updates to your data dictionary.",
        "to_update": to_update
    }
