import re

from fastapi import  Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, inspect, MetaData, Table,text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from cassandra.query import BatchStatement
import datetime
from contextlib import contextmanager
from pydantic import BaseModel
import uuid

import json
from fastapi import APIRouter
from typing import List

import logging

import settings
from models.models import AccessCredentials,MappedVariables, DataDictionaryTerms, DataDictionaries,SiteConfig,\
    TransmissionHistory, ExtractsQueries
from database import database
from serializers.dictionary_mapper_serializer import mapped_variable_entity,mapped_variable_list_entity
from serializers.data_dictionary_serializer import data_dictionary_list_entity, data_dictionary_terms_list_entity
from settings import settings


class QueryModel(BaseModel):
    query: str


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)




router = APIRouter()


# # Create an inspector object to inspect the database
engine = None
inspector = None
metadata = None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def createEngine():
    global engine, inspector, metadata
    log.info('===== start creating an engine =====')

    try:
        credentials = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        if credentials["conn_type"] not in ["csv", "api"]:
            connection_string = credentials
            engine = create_engine(connection_string["conn_string"])

            inspector = inspect(engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            log.info('===== Database reflected ====')

    except SQLAlchemyError as e:
        # Log the error or handle it as needed
        log.error('===== Database not reflected ==== ERROR:', str(e))
        raise HTTPException(status_code=500, detail="Database connection error"+str(e))

@contextmanager
def get_db():
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()

@router.on_event("startup")
async def startup_event():
    createEngine()
    if engine:
        SessionLocal.configure(bind=engine)
    # else:
    #     raise HTTPException(status_code=500, detail="Failed to initialize database engine")



@router.get('/base_schemas')
async def base_schemas():
    try:
        schemas = DataDictionaries.objects().all()
        schemas =data_dictionary_list_entity(schemas)
        return schemas
    except Exception as e:
        log.error('System ran into an error --> ', e)
        return e


@router.get('/base_schema_variables/{baselookup}')
async def base_schema_variables(baselookup: str):
    try:
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()

        dictionary = DataDictionaryTerms.objects.filter(dictionary=baselookup).allow_filtering()
        dictionary = data_dictionary_terms_list_entity(dictionary)

        schemas = []

        base_variables = []
        for i in dictionary:
            configs = MappedVariables.objects.filter(base_variable_mapped_to=i['term'],base_repository=baselookup,
                                                     source_system_id=source_system['id']).allow_filtering()

            configs = mapped_variable_list_entity(configs)

            results = []
            for row in configs:
                results.append(row)
            matchedVariable = False if not results else True

            base_variables.append({'variable':i['term'], 'matched':matchedVariable})

        baseSchemaObj = {}
        baseSchemaObj["schema"] = baselookup
        baseSchemaObj["base_variables"] = base_variables

        schemas.append(baseSchemaObj)
        return schemas
    except Exception as e:
        log.error('System ran into an error fetching base_schema_variables --->', e)
        return e


@router.get('/base_variables/{base_lookup}')
async def base_variables_lookup(base_lookup: str):

    try:
        dictionary_terms = DataDictionaryTerms.objects.filter(dictionary=base_lookup).all()
        base_variables = []
        for term in dictionary_terms:
            base_variables.append({"term":term.term, "datatype":term.data_type, "is_required":term.is_required})

        return {"data":base_variables}
    except Exception as e:
        log.error('System ran into an error fetching base_variables --->', e)
        return e


@router.get('/get_database_columns')
async def get_database_columns():
    if metadata:
        try:
            dbTablesAndColumns={}

            table_names = metadata.tables.keys()

            for table_name in table_names:
                # Load the table schema from MetaData
                table = Table(table_name, metadata, autoload_with=engine)

                getcolumnnames = []
                # add epmty string as part of options
                getcolumnnames.append({"name": "", "type": "-"})
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
async def add_mapped_variables(baselookup:str, variables:List[object]):
    try:
        #delete existing configs for base repo
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        existingMappings = MappedVariables.objects(base_repository=baselookup,source_system_id=source_system['id']).allow_filtering().all()
        for mapping in existingMappings:
            mapping.delete()

        for variableSet in variables:
            MappedVariables.create(tablename=variableSet["tablename"],columnname=variableSet["columnname"],
                                    datatype=variableSet["datatype"], base_repository=variableSet["base_repository"],
                                    base_variable_mapped_to=variableSet["base_variable_mapped_to"],
                                   join_by=variableSet["join_by"], source_system_id=source_system['id'])

        # after saving mappings, generate query from them and save
        extract_source_data_query = generate_query(baselookup)

        existingQuery = ExtractsQueries.objects(base_repository=baselookup,
                                                source_system_id=source_system['id']).allow_filtering().first()

        if existingQuery:
            ExtractsQueries.objects(id=existingQuery["id"]).update(
                query=extract_source_data_query
            )
        else:
            ExtractsQueries.create(query=extract_source_data_query,
                                   base_repository=baselookup,
                                   source_system_id=source_system['id'])

        return {"data":"Successfully added Mapped Variables"}
    except Exception as e:
        return {"status":500, "message":e}


@router.post('/add_query/{baselookup}')
async def add_query(baselookup:str, customquery:QueryModel):
    try:
        #update existing configs for base repo
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()

        dictTerms = DataDictionaryTerms.objects.filter(dictionary=baselookup).allow_filtering().all()

        # add blank mappings
        existingMappings = MappedVariables.objects(base_repository=baselookup,
                                                   source_system_id=source_system['id']).allow_filtering().all()
        for mapping in existingMappings:
            mapping.delete()

        for variable in dictTerms:
            MappedVariables.create(tablename="-", columnname="-",
                                   datatype="-", base_repository=baselookup,
                                   base_variable_mapped_to=variable["term"],
                                   join_by="-", source_system_id=source_system['id'])

        # add custom query
        existingQuery = ExtractsQueries.objects(base_repository=baselookup,source_system_id=source_system['id']).allow_filtering().first()
        if existingQuery:
            ExtractsQueries.objects(id=existingQuery["id"]).update(
                query=customquery.query
            )
        else:
            ExtractsQueries.create(query=customquery.query,
                                     base_repository=baselookup,
                                       source_system_id=source_system['id'])
        return {"data":"Custom Query for Variables successfully added"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error adding query for source system:" + str(e))


@router.post('/test/mapped_variables/{baselookup}')
async def test_mapped_variables(baselookup:str, variables:List[object], db_session: Session = Depends(get_db)):
    try:
        extractQuery = text(generate_test_query(baselookup, variables))

        with db_session as session:
            result = session.execute(extractQuery)

            columns = result.keys()
            baseRepoLoaded = [dict(zip(columns, row)) for row in result]

            processed_results = [result for result in baseRepoLoaded]

        list_of_issues = validateMandatoryFields(baselookup, variables, processed_results)

        return {"data":list_of_issues}
    except Exception as e:
        # return {"status":500, "message":e
        raise HTTPException(status_code=500, detail="Error testing mappings on source system:" + str(e))


def validateMandatoryFields(baselookup:str, variables:List[object], processed_results:List[object]):

    list_of_issues = []
    for variableSet in variables:
        if variableSet["base_variable_mapped_to"] != "PrimaryTableId":
            filteredData = [obj[variableSet["base_variable_mapped_to"].lower()] for obj in processed_results]

            dictTerms = DataDictionaryTerms.objects.filter(dictionary=baselookup, term=variableSet["base_variable_mapped_to"]).allow_filtering().first()

            if dictTerms["is_required"] == True:
                if "" in filteredData :
                    print()
                if None in filteredData:
                    print()
                if "N/A" in filteredData:
                    print()
                if "NULL" in filteredData:
                    print()

                if "" in filteredData or "N/A" in filteredData or "NULL" in filteredData:
                    issueObj = {"base_variable": variableSet["base_variable_mapped_to"],
                                "issue": "*Variable is Mandatory. Data is expected in all records.",
                                "column_mapped": variableSet["columnname"],
                                "recommended_solution": "Ensure all records have this data"}
                    list_of_issues.append(issueObj)
    return list_of_issues


def generate_test_query(baselookup:str, variableSet:List[object]):
    try:
        mapped_columns = []
        mapped_joins = []

        primaryTableDetails = [mapping for mapping in variableSet if mapping["base_variable_mapped_to"] == 'PrimaryTableId']
        for variableMapped in variableSet:
            if variableMapped["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(variableMapped["tablename"]+ "."+variableMapped["columnname"] +" as '"+variableMapped["base_variable_mapped_to"]+"' ")
                if all(variableMapped["tablename"]+"." not in s for s in mapped_joins):
                    if variableMapped["tablename"] != primaryTableDetails[0]['tablename']:
                        mapped_joins.append(" LEFT JOIN "+variableMapped["tablename"] + " ON " +
                        primaryTableDetails[0]["tablename"].strip() + "." + primaryTableDetails[0]["columnname"].strip() +
                        " = " + variableMapped["tablename"].strip() + "." + variableMapped["join_by"].strip())

        columns = ", ".join(mapped_columns)
        joins = ", ".join(mapped_joins)

        site_config = SiteConfig.objects.filter(is_active=True).allow_filtering().first()
        mappedSiteCode = [mapping for mapping in variableSet if mapping["base_variable_mapped_to"] == 'FacilityID']

        query = f"SELECT {columns} from {primaryTableDetails[0]['tablename']} {joins.replace(',','')}" \
                f" where  {mappedSiteCode[0]['tablename']}.{mappedSiteCode[0]['columnname']} = {site_config['site_code']}"
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e


@router.post('/test/query/mapped_variables/{baselookup}')
async def test_query_mapped_variables(baselookup:str, customquery:QueryModel, db_session: Session = Depends(get_db)):
    try:
        list_of_issues = []

        dictionary_terms = DataDictionaryTerms.objects.filter(dictionary=baselookup).allow_filtering()
        dictionary_terms = data_dictionary_terms_list_entity(dictionary_terms)
        terms_list = [term["term"].lower() for term in dictionary_terms]

        with db_session as session:
            result = session.execute(text(customquery.query))

            columnsProvided = [col.lower() for col in result.keys()]# columns provided in query
            baseRepoLoaded = [dict(zip(columnsProvided, row)) for row in result]

            processed_results = [result for result in baseRepoLoaded]

            # check if base variable terms are all in the columns provided in the custom query
            for variable in terms_list:
                if variable not in columnsProvided:
                    issueObj = {"base_variable": "?",
                                "issue": "*Variable is missing but is expected in the list of base variables.",
                                "column_mapped": variable,
                                "recommended_solution": "Ensure all expected base variables are in the query provided"}
                    list_of_issues.append(issueObj)

            # check for any unnecessary columns provided in query that are not base variable terms
            for variable in columnsProvided:
                if variable.lower() not in terms_list:
                    issueObj = {"base_variable": "?",
                                "issue": "*Variable is not part of the expected base variables.",
                                "column_mapped": variable,
                                "recommended_solution": "Ensure only variables that are listed as base variables are in the query provided"}
                    list_of_issues.append(issueObj)

        return {"data":list_of_issues}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error testing mappings on source system:" + str(e))


def generate_query(baselookup:str):
    try:
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()

        configs = MappedVariables.objects.filter(base_repository=baselookup,source_system_id=source_system['id']).allow_filtering()
        configs = mapped_variable_list_entity(configs)

        primaryTableDetails = MappedVariables.objects.filter(base_repository=baselookup, base_variable_mapped_to='PrimaryTableId',
                                                             source_system_id=source_system['id']).allow_filtering().first()

        mapped_columns = []
        mapped_joins = []

        for conf in configs:
            if conf["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(conf["tablename"]+ "."+conf["columnname"] +" as "+conf["base_variable_mapped_to"]+" ")
                if all(conf["tablename"]+"." not in s for s in mapped_joins):
                    if conf["tablename"] != primaryTableDetails['tablename']:
                        mapped_joins.append(" LEFT JOIN "+conf["tablename"] + " ON " + primaryTableDetails["tablename"].strip() + "." + primaryTableDetails["columnname"].strip() +
                        " = " + conf["tablename"].strip() + "." + conf["join_by"].strip())

        columns = ", ".join(mapped_columns)
        joins = ", ".join(mapped_joins)

        site_config = SiteConfig.objects.filter(is_active=True).allow_filtering().first()
        mappedSiteCode = MappedVariables.objects.filter(base_repository=baselookup, base_variable_mapped_to='FacilityID',
                                                        source_system_id=source_system['id']).allow_filtering().first()

        query = f"SELECT {columns} from {primaryTableDetails['tablename']} {joins.replace(',','')}" \
                f" where  {mappedSiteCode['tablename']}.{mappedSiteCode['columnname']} = {site_config['site_code']}"
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e

def convert_none_to_null(data):
    if isinstance(data, dict):
        return {k: convert_none_to_null(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_none_to_null(item) for item in data]
    elif data is None:
        return None # This will be converted to null in JSON
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


async def load_data(baselookup:str, websocket: WebSocket):
    try:
        cass_session = database.cassandra_session_factory()

        # system config data
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        site_config = SiteConfig.objects.filter(is_active=True).allow_filtering().first()

        existingQuery = ExtractsQueries.objects(base_repository=baselookup,
                                                source_system_id=source_system['id']).allow_filtering().first()
        extract_source_data_query = existingQuery["query"]

        # ------ started extraction -------

        loadedHistory = TransmissionHistory(usl_repository_name=baselookup, action="Loading",
                            facility=f'{site_config["site_name"]}-{site_config["site_code"]}',
                            source_system_id=source_system['id'],
                            source_system_name=site_config['primary_system'],
                            ended_at=None,
                            manifest_id=None).save()

        processed_results=[]
        if source_system["conn_type"] not in ["csv","api"]:
            # extract data from source DB
            with get_db() as session:

                result = session.execute(text(extract_source_data_query))

                columns = result.keys()
                baseRepoLoaded = [dict(zip(columns,row)) for row in result]

                processed_results=[result for result in baseRepoLoaded]
        else:
            # extract data from imported csv/api schema
            baseRepoLoaded = cass_session.execute(extract_source_data_query)
            processed_results = [result for result in baseRepoLoaded]

        # ------ --------------- -------
        # ------ started loading -------

        if len(processed_results) > 0:
            # clear base repo data in preparation for inserting new data
            cass_session.execute("TRUNCATE TABLE %s;" % (baselookup))

            count_inserted = 0
            batch_size = 100

            for i in range(0, len(processed_results), batch_size):
                batch = processed_results[i:i + batch_size]
                batch_stmt = BatchStatement()
                for data in batch:

                    quoted_values = [
                        'NULL' if value is None
                        else f'{int(value)}' if (DataDictionaryTerms.objects.filter(dictionary=baselookup,term=key).allow_filtering().first()[ "data_type"] == "INT")
                        else f"'{value}'" if isinstance(value, str)
                        else f"'{value.strftime('%Y-%m-%d')}'" if isinstance(value, datetime.date)  # Convert date to string
                        else f"'{value}'" if (DataDictionaryTerms.objects.filter(dictionary=baselookup,term=key).allow_filtering().first()["data_type"] =="NVARCHAR")
                        else str(value)
                        for key, value in data.items()
                    ]

                    idColumn = baselookup + "_id"

                    query = f"""
                               INSERT INTO {baselookup} ({idColumn}, {", ".join(tuple(data.keys()))})
                               VALUES (uuid(), {', '.join(quoted_values)})
                           """
                    print("query -->", query)
                    prepared_stm = cass_session.prepare(query)
                    batch_stmt.add(prepared_stm)
                    # add up records
                    count_inserted += 1

                cass_session.execute(batch_stmt)

                await websocket.send_text(f"{count_inserted}")
                log.info("+++++++ data batch +++++++")
                log.info(f"+++++++ step i : count_inserted +++++++ {count_inserted} records")

            log.info("+++++++ USL Base Repository Data saved +++++++")

        # end batch
        cass_session.cluster.shutdown()
        baseRepoLoaded_json_data = json.dumps(baseRepoLoaded, default=str)

        # Send the JSON string over the WebSocket
        await websocket.send_text(baseRepoLoaded_json_data)
        await websocket.close()
        # ended loading
        # TO DO - update history
        # loadedHistory.ended_at=datetime.utcnow()
        # loadedHistory.save()
        # TransmissionHistory.objects(id=loadedHistory.id).update(ended_at=datetime.utcnow())

        # return {"data": [expected_variables_dqa(data, baselookup) for data in baseRepoLoaded]}
        return {"data": baseRepoLoaded}
    except Exception as e:
        log.error("Error loading data ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error loading data:" + str(e))


def expected_variables_dqa(data, lookup):
    valid_match = True
    try:
        if dictionary := DataDictionaries.objects.filter(name=lookup).first():
            dictionary_terms = DataDictionaryTerms.objects.filter(dictionary_id=dictionary.id).all()
            for term in dictionary_terms:
                column_data = data.get(term.term)
                if is_valid_regex(term.expected_values) and not re.match(pattern=term.expected_values, string=str(column_data)):
                    valid_match = False

    except Exception as e:
        log.error(f"Error {str(e)}")

    data['valid_match'] = valid_match
    return data


def is_valid_regex(pattern):
    try:
        re.compile(pattern)
        return True
    except re.error:
        return False

def source_total_count(baselookup:str):
    try:
        configs = MappedVariables.objects.filter(base_repository=baselookup).allow_filtering()
        configs = mapped_variable_list_entity(configs)

        primaryTableDetails = MappedVariables.objects.filter(base_repository=baselookup, base_variable_mapped_to='PrimaryTableId').allow_filtering().first()

        mapped_columns = []
        mapped_joins = []

        for conf in configs:
            if conf["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(conf["tablename"]+ "."+conf["columnname"] +" as '"+conf["base_variable_mapped_to"]+"' ")
                if all(conf["tablename"]+"." not in s for s in mapped_joins):
                    if conf["tablename"] != primaryTableDetails['tablename']:
                        mapped_joins.append(" LEFT JOIN "+conf["tablename"] + " ON " + primaryTableDetails["tablename"].strip() + "." + primaryTableDetails["columnname"].strip() +
                        " = " + conf["tablename"].strip() + "." + conf["join_by"].strip())

        columns = ", ".join(mapped_columns)
        joins = ", ".join(mapped_joins)

        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        site_config = SiteConfig.objects.filter(is_active=True).allow_filtering().first()
        mappedSiteCode = MappedVariables.objects.filter(base_repository=baselookup, base_variable_mapped_to='FacilityID',
                                                        source_system_id=source_system['id']).allow_filtering().first()

        query = f"SELECT count(*) as count from {primaryTableDetails['tablename']} {joins.replace(',','')}" \
                f" where  {mappedSiteCode['tablename']}.{mappedSiteCode['columnname']} = {site_config['site_code']}"

        log.info("++++++++++ Successfully generated count query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e

def inserted_total_count(baselookup:str):
    try:
        cass_session = database.cassandra_session_factory()

        totalRecordsquery = f"SELECT COUNT(*) as count FROM {baselookup}"
        totalRecordsresult = cass_session.execute(totalRecordsquery)
        cass_session.cluster.shutdown()

        insertedCount = totalRecordsresult[0]['count']
        print("insertedCount--->",insertedCount)

        return insertedCount
    except Exception as e:
        log.error("Error getting total inserted query. ERROR: ==> %s", str(e))
        return 0

@router.websocket("/ws/load/progress/{baselookup}")
async def progress_websocket_endpoint(baselookup: str, websocket: WebSocket, db_session: Session = Depends(get_db)):
    await websocket.accept()
    print("websocket manifest -->", baselookup)

    try:

        while True:
            data = await websocket.receive_text()
            baseRepo = data
            print("websocket manifest -->", baseRepo)
            await load_data(baselookup,websocket)
    except WebSocketDisconnect:
        log.error("Client disconnected")
        await websocket.close()
    except Exception as e:
        log.error("Websocket error ==> %s", str(e))
        await websocket.close()