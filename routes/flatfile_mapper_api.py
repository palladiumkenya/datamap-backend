from sqlalchemy import create_engine, inspect, MetaData, Table,text

import json
from fastapi import Depends, APIRouter,HTTPException
from typing import List

import logging

from sqlalchemy.orm import Session

import settings
from database.database import get_db, execute_raw_data_query
from models.models import AccessCredentials,MappedVariables, DataDictionaryTerms, DataDictionaries,SiteConfig,\
    TransmissionHistory, ExtractsQueries
from database import database
from serializers.dictionary_mapper_serializer import mapped_variable_entity,mapped_variable_list_entity
from routes.dictionary_mapper_api import validateMandatoryFields


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


router = APIRouter()


@router.get('/columns/{conn_type}')
async def get_columns(conn_type: str, db: Session = Depends(get_db)):
    credentials = db.query(AccessCredentials).filter(AccessCredentials.is_active==True).first()
    if credentials:
        try:
            tableName = f"{credentials.name.lower()}_{conn_type}_extract"
            query = text(f"""SELECT column_name FROM information_schema.columns 
            WHERE table_name='{tableName}'""")
            rows = execute_raw_data_query(query)
            dbTablesAndColumns = [row["column_name"] for row in rows]
            print(rows)

            return {"data": dbTablesAndColumns}
        except Exception as e:
            log.error('Error getting csv columns: --->', e)
            raise HTTPException(status_code=500, detail='Error reflecting source database')
    else:
        log.error('Error getting csv columns: --->')
        raise HTTPException(status_code=500, detail='Error getting csv columns')


@router.post('/add/{conn_type}/mapped_variables/{baselookup}')
async def add_mapped_variables(conn_type: str, baselookup: str, variables: List[object], db: Session = Depends(get_db)):
    try:
        #delete existing configs for base repo
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active==True).first()
        existingMappings = db.query(MappedVariables).filter(
            MappedVariables.base_repository==baselookup,
            MappedVariables.source_system_id==source_system.id
        ).delete()
        db.commit()

        credentials = db.query(AccessCredentials).filter(AccessCredentials.is_active==True).first()

        for variableSet in variables:
            variables = MappedVariables(
                tablename=f"{credentials.name.lower()}_{conn_type}_extract",
                columnname=variableSet["columnname"],
                datatype="-", base_repository=baselookup,
                base_variable_mapped_to=variableSet["base_variable_mapped_to"],
                join_by="-", source_system_id=source_system.id
            )
            db.add(variables)
        db.commit()

        # after saving mappings, generate query from them and save
        extract_source_data_query = generate_flatfile_query(baselookup, db)

        existingQuery = db.query(ExtractsQueries).filter(
            ExtractsQueries.base_repository==baselookup,
            ExtractsQueries.source_system_id==source_system.id
        ).first()

        if existingQuery:
            existing_queries = db.query(ExtractsQueries).filter(ExtractsQueries.id==existingQuery.id).first()
            existing_queries.query = f'{extract_source_data_query}'
        else:
            new_query = ExtractsQueries(
                query=f'{extract_source_data_query}',
                base_repository=baselookup,
                source_system_id=source_system.id
            )
            db.add(new_query)
        db.commit()

        return {"data": "Successfully added Mapped Variables"}
    except Exception as e:
        log.error("Error adding mappings for source system:" + str(e))
        raise HTTPException(status_code=500, detail="Error adding mappings for source system:" + str(e))


@router.post('/test/{conn_type}/mapped_variables/{baselookup}')
async def test_mapped_variables(conn_type:str, baselookup: str, variables: List[object], db: Session = Depends(get_db)):
    try:
        extractQuery = generate_test_query(conn_type, variables, db)

        baseRepoLoaded = execute_raw_data_query(text(extractQuery))

        processed_results = [result for result in baseRepoLoaded]

        # check if base variable terms are all in the columns provided in the custom query
        list_of_issues = validateMandatoryFields(baselookup, variables, processed_results, db)

        return {"data": list_of_issues}
    except Exception as e:
        log.error("Error testing mappings. ERROR: ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error testing mappings on source system:" + str(e))


def generate_flatfile_query(baselookup: str, db):
    try:
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()

        configs = db.query(MappedVariables).filter(
            MappedVariables.base_repository==baselookup,
            MappedVariables.source_system_id==source_system.id
        ).all()
        configs = mapped_variable_list_entity(configs)

        mapped_columns = []
        mapped_joins = []

        for conf in configs:
            if conf["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(f'{conf["columnname"]}  as "{conf["base_variable_mapped_to"]}" ')

        columns = ", ".join(mapped_columns)

        site_config = db.query(SiteConfig).filter(SiteConfig.is_active==True).first()
        mappedSiteCode = db.query(MappedVariables).filter(
            MappedVariables.base_repository==baselookup,
            MappedVariables.base_variable_mapped_to=='FacilityID',
            MappedVariables.source_system_id==source_system.id
        ).first()

        tableName = configs[0]["tablename"]
        query = text(f"SELECT {columns} from {tableName} ")
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e


def generate_test_query(conn_type: str, variableSet: List[object], db):
    try:
        credentials = db.query(AccessCredentials).filter(AccessCredentials.is_active==True).first()

        tableName = f"{credentials.name.lower()}_{conn_type}_extract"

        mapped_columns = []

        for variableMapped in variableSet:
            if variableMapped["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(variableMapped["columnname"] +" as \""+variableMapped["base_variable_mapped_to"]+"\" ")

        columns = ", ".join(mapped_columns)

        query = f"SELECT {columns} from {tableName}"
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e
