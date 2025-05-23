import uuid

from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, inspect, MetaData, Table, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
import datetime
from contextlib import contextmanager
from pydantic import BaseModel
import uuid

import json
from fastapi import APIRouter
from typing import List

import logging

from database.database import execute_data_query, get_db as get_main_db, execute_query, engine as postgres_engine
from database.source_system_database import get_source_db, engine as source_db_engine, createSourceDbEngine,\
    SessionLocal,metadata

from models.models import AccessCredentials, MappedVariables, DataDictionaryTerms, DataDictionaries, SiteConfig, \
    TransmissionHistory, ExtractsQueries
from models import models
from serializers.dictionary_mapper_serializer import mapped_variable_entity, mapped_variable_list_entity
from serializers.data_dictionary_serializer import data_dictionary_list_entity, data_dictionary_terms_list_entity

class QueryModel(BaseModel):
    query: str


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

router = APIRouter()



class EngineState:
    def __init__(self):
        self.sourceEngine = None
        self.metadata = None

engine_state = EngineState()

async def createEngine():
    engine_state.sourceEngine = createSourceDbEngine()
    if engine_state.sourceEngine:
        engine_state.metadata = MetaData()
        engine_state.metadata.reflect(bind=engine_state.sourceEngine)
        SessionLocal.configure(bind=engine_state.sourceEngine)
def get_engine_state():
    return engine_state

@router.on_event("startup")
async def startup_event():
    await createEngine()
#     getEngineCreated = createSourceDbEngine()
#     if getEngineCreated:
#         metadata = MetaData()
#         metadata.reflect(bind=getEngineCreated)
#         SessionLocal.configure(bind=getEngineCreated)

#     else:
#         raise HTTPException(status_code=500, detail="Failed to initialize database engine")


def databaseConnType(db):
    try:
        credentials = db.query(AccessCredentials).filter(AccessCredentials.is_active==True).first()
        if credentials.conn_type not in ["csv", "api"]:
            return True
        else:
            return False
    except Exception as e:
        log.error('Error reflecting source database: --->')
        raise HTTPException(status_code=500, detail='Error reflecting source database')


@router.get('/base_schemas')
async def base_schemas(db: Session = Depends(get_main_db), engine_state: EngineState = Depends(get_engine_state)):
    try:
        # create engine if conn type id mssql/mysql/postgres and engine is not created
        if databaseConnType(db):
            await createEngine()

        # get the dictionary base repositories
        schemas = db.query(DataDictionaries).all()
        schemas = data_dictionary_list_entity(schemas)

        return schemas
    except Exception as e:
        log.error('System ran into an error --> ', e)
        return e


@router.get('/base_schema_variables/{baselookup}')
async def base_schema_variables(baselookup: str, db: Session = Depends(get_main_db)):
    try:
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()

        dictionary = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary == baselookup).all()
        dictionary = data_dictionary_terms_list_entity(dictionary)

        schemas = []

        base_variables = []
        for i in dictionary:
            configs = db.query(MappedVariables).filter(
                MappedVariables.base_variable_mapped_to == i['term'],
                MappedVariables.base_repository == baselookup,
                MappedVariables.source_system_id == source_system.id
            ).all()

            configs = mapped_variable_list_entity(configs)

            results = []
            for row in configs:
                results.append(row)
            matchedVariable = False if not results else True

            base_variables.append({'variable': i['term'], 'matched': matchedVariable})

        baseSchemaObj = {"schema": baselookup, "base_variables": base_variables}

        schemas.append(baseSchemaObj)
        return schemas
    except Exception as e:
        log.error('System ran into an error fetching base_schema_variables --->', e)
        return e


@router.get('/base_variables/{base_lookup}')
async def base_variables_lookup(base_lookup: str, db: Session = Depends(get_main_db)):
    try:
        dictionary_terms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary == base_lookup).all()
        base_variables = []
        for term in dictionary_terms:
            base_variables.append({"term": term.term, "datatype": term.data_type, "is_required": term.is_required})

        return {"data": base_variables}
    except Exception as e:
        log.error('System ran into an error fetching base_variables --->', e)
        return e


@router.get('/get_database_columns')
async def get_database_columns(engine_state: EngineState = Depends(get_engine_state)):
    if engine_state.metadata:
        try:
            dbTablesAndColumns = {}

            table_names = engine_state.metadata.tables.keys()

            for table_name in table_names:
                # Load the table schema from MetaData
                table = Table(table_name, engine_state.metadata, autoload_with=source_db_engine)

                getcolumnnames = [{"name": "", "type": "-"}]
                # add epmty string as part of options
                for column in table.columns:
                    getcolumnnames.append({"name": column.name, "type": str(column.type)})

                dbTablesAndColumns[table_name] = getcolumnnames

            return dbTablesAndColumns
        except SQLAlchemyError as e:
            log.error('Error reflecting database: --->', e)
    else:
        log.error('Error reflecting source database: --->')
        raise HTTPException(status_code=500, detail='Error reflecting source database')


@router.post('/add_mapped_variables/{baselookup}')
async def add_mapped_variables(baselookup: str, variables: List[object], db: Session = Depends(get_main_db)):
    try:
        # delete existing configs for base repo
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()
        db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.source_system_id == source_system.id
        ).delete()

        for variableSet in variables:
            mapped_variable = MappedVariables(tablename=variableSet["tablename"], columnname=variableSet["columnname"],
                                              datatype=variableSet["datatype"],
                                              base_repository=variableSet["base_repository"],
                                              base_variable_mapped_to=variableSet["base_variable_mapped_to"],
                                              join_by=variableSet["join_by"], source_system_id=source_system.id)
            db.add(mapped_variable)
        db.commit()

        # after saving mappings, generate query from them and save
        extract_source_data_query = generate_query(baselookup, db)

        existingQuery = db.query(ExtractsQueries).filter(ExtractsQueries.base_repository==baselookup,
                                                ExtractsQueries.source_system_id==source_system.id).first()

        if existingQuery:
            extract = db.query(ExtractsQueries).filter(id=existingQuery.id).first()
            extract.query = extract_source_data_query
            
        else:
            extract = ExtractsQueries(query=extract_source_data_query,
                            base_repository=baselookup,
                            source_system_id=source_system.id)
            db.add(extract)
        db.commit()

        return {"data": "Successfully added Mapped Variables"}

    except Exception as e:
        print(str(e))
        return {"status": 500, "message": e}


@router.post('/add_query/{baselookup}')
async def add_query(baselookup:str, customquery:QueryModel, db: Session = Depends(get_main_db)):
    try:
        #update existing configs for base repo
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active==True).first()

        dictTerms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary==baselookup).all()
        dictTerms = data_dictionary_terms_list_entity(dictTerms)

        # clear previous mappings and add blank mappings
        existingMappings = db.query(MappedVariables).filter(MappedVariables.base_repository==baselookup,
                                                   MappedVariables.source_system_id==source_system.id).delete()
        db.commit()


        for variable in dictTerms:
            new_variables = MappedVariables(tablename="-", columnname="-",
                                   datatype="-", base_repository=baselookup,
                                   base_variable_mapped_to=variable["term"],
                                   join_by="-", source_system_id=source_system.id)
            db.add(new_variables)
        db.commit()

        # add custom query
        db.query(ExtractsQueries).filter(ExtractsQueries.base_repository==baselookup, ExtractsQueries.source_system_id==source_system.id).delete()
        # TODO : update instead of delete if existingQuery:
        #     existingQuery.query = customquery.query
        #     existingQuery.updated_at = datetime.now(timezone.utc)
        #     db.add(existingQuery)
        #     db.commit()
        new_extract_query = ExtractsQueries(query=customquery.query,
                        base_repository=baselookup,
                        source_system_id=source_system.id)
        db.add(new_extract_query)
        db.commit()
        return {"data":"Custom Query for Variables successfully added"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error adding query for source system:" + str(e))


@router.post('/test/mapped_variables/{baselookup}')
async def test_mapped_variables(baselookup: str, variables: List[object], db_session: Session = Depends(get_source_db),
                                db: Session = Depends(get_main_db)):
    try:
        extractQuery = text(generate_test_query(baselookup, variables, db))
        print(extractQuery)

        with db_session as session:
            result = session.execute(extractQuery)

            columns = result.keys()
            baseRepoLoaded = [dict(zip(columns, row)) for row in result]

            processed_results = [result for result in baseRepoLoaded]

        list_of_issues = validateMandatoryFields(baselookup, variables, processed_results, db)

        return {"data": list_of_issues}
    except Exception as e:
        # return {"status":500, "message":e
        raise HTTPException(status_code=500, detail="Error testing mappings on source system:" + str(e))


def validateMandatoryFields(baselookup: str, variables: List[object], processed_results: List[object], db):
    list_of_issues = []
    for variableSet in variables:
        if variableSet["base_variable_mapped_to"] != "PrimaryTableId":
            filteredData = [obj[variableSet["base_variable_mapped_to"]] for obj in processed_results]

            dictTerms = db.query(DataDictionaryTerms).filter(
                DataDictionaryTerms.dictionary == baselookup,
                DataDictionaryTerms.term == variableSet["base_variable_mapped_to"]
            ).first()

            if dictTerms.is_required:
                if "" in filteredData or "N/A" in filteredData or "NULL" in filteredData:
                    issueObj = {"base_variable": variableSet["base_variable_mapped_to"],
                                "issue": "*Variable is Mandatory. Data is expected in all records.",
                                "column_mapped": variableSet["columnname"],
                                "recommended_solution": "Ensure all records have this data"}
                    list_of_issues.append(issueObj)
    return list_of_issues


def generate_test_query(baselookup: str, variableSet: List[object], db):
    try:
        mapped_columns = []
        mapped_joins = []

        primaryTableDetails = [mapping for mapping in variableSet if
                               mapping["base_variable_mapped_to"] == 'PrimaryTableId']
        for variableMapped in variableSet:
            if variableMapped["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(variableMapped["tablename"]+ "."+variableMapped["columnname"].lower() +" as \""+variableMapped["base_variable_mapped_to"]+"\" ")
                if all(variableMapped["tablename"]+"." not in s for s in mapped_joins):
                    if variableMapped["tablename"] != primaryTableDetails[0]['tablename']:
                        mapped_joins.append(" LEFT JOIN " + variableMapped["tablename"] + " ON " +
                                            primaryTableDetails[0]["tablename"].strip() + "." + primaryTableDetails[0][
                                                "columnname"].strip() +
                                            " = " + variableMapped["tablename"].strip() + "." + variableMapped[
                                                "join_by"].strip())

        columns = ", ".join(mapped_columns)
        joins = ", ".join(mapped_joins)

        site_config = db.query(SiteConfig).filter(SiteConfig.is_active == True).first()
        mappedSiteCode = [mapping for mapping in variableSet if mapping["base_variable_mapped_to"] == 'FacilityID']

        query = f"SELECT {columns} from {primaryTableDetails[0]['tablename']} {joins.replace(',', '')}" \
                f" where  CAST({mappedSiteCode[0]['tablename']}.{mappedSiteCode[0]['columnname']} AS INT) = {site_config.site_code}"
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e


@router.post('/test/query/mapped_variables/{baselookup}')
async def test_query_mapped_variables(baselookup: str, customquery: QueryModel, db_session: Session = Depends(get_source_db),  db: Session = Depends(get_main_db)):
    try:
        list_of_issues = []

        dictionary_terms = db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary==baselookup).all()
        dictionary_terms = data_dictionary_terms_list_entity(dictionary_terms)
        terms_list = [term["term"] for term in dictionary_terms]

        with db_session as session:
            result = session.execute(text(customquery.query))

            columnsProvided = [col for col in result.keys()]# columns provided in query
            baseRepoLoaded = [dict(zip(columnsProvided, row)) for row in result]

            processed_results = [result for result in baseRepoLoaded]

            # check if base variable terms are all in the columns provided in the custom query
            for variable in terms_list:
                if variable not in columnsProvided:
                    issueObj = {"base_variable": "?",
                                "issue": "*Variable is missing but is expected in the list of base variables.",
                                "column_mapped": variable,
                                "recommended_solution": "Ensure all expected base variables are in the query provided and match what is shared  above"}
                    list_of_issues.append(issueObj)

            # check for any unnecessary columns provided in query that are not base variable terms
            for variable in columnsProvided:
                if variable not in terms_list:
                    issueObj = {"base_variable": "?",
                                "issue": "*Variable is not part of the expected base variables.",
                                "column_mapped": variable,
                                "recommended_solution": "Ensure only variables that are listed as base variables are in the query provided"}
                    list_of_issues.append(issueObj)

        return {"data":list_of_issues}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error testing mappings on source system:" + str(e))


def generate_query(baselookup: str, db):
    try:
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()

        configs = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.source_system_id == source_system.id).all()
        configs = mapped_variable_list_entity(configs)

        primaryTableDetails = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.base_variable_mapped_to == 'PrimaryTableId',
            MappedVariables.source_system_id == source_system.id
        ).first()

        mapped_columns = []
        mapped_joins = []

        for conf in configs:
            if conf['base_variable_mapped_to'] != 'PrimaryTableId':
                mapped_columns.append(
                    conf['tablename'] + "." + conf['columnname'] + " as \"" + conf['base_variable_mapped_to'] + "\" ")
                if all(conf['tablename'] + "." not in s for s in mapped_joins):
                    if conf['tablename'] != primaryTableDetails.tablename:
                        mapped_joins.append(
                            f" LEFT JOIN {conf['tablename']} ON {primaryTableDetails.tablename.strip()}.{primaryTableDetails.columnname.strip()} = {conf['tablename'].strip()}.{conf['join_by'].strip()}")

        columns = ", ".join(mapped_columns)
        joins = ", ".join(mapped_joins)

        site_config = db.query(SiteConfig).filter(SiteConfig.is_active == True).first()
        mappedSiteCode = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.base_variable_mapped_to == 'FacilityID',
            MappedVariables.source_system_id == source_system.id
        ).first()

        query = f"""SELECT {columns} from {primaryTableDetails.tablename} {joins.replace(',', '')}
                 WHERE CAST({mappedSiteCode.tablename}.{mappedSiteCode.columnname} AS INT) = {site_config.site_code}"""
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))
        raise e


def convert_none_to_null(data):
    if isinstance(data, dict):
        return {k: convert_none_to_null(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_none_to_null(item) for item in data]
    elif data is None:
        return None  # This will be converted to null in JSON
    else:
        return data


def convert_datetime_to_iso(data):
    if isinstance(data, dict):
        return {k: convert_datetime_to_iso(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_datetime_to_iso(item) for item in data]
    elif isinstance(data, datetime.date):
        return data.isoformat()
    else:
        return data







