from fastapi import  Depends, HTTPException, WebSocket, BackgroundTasks
import time
from sqlalchemy import create_engine, inspect, MetaData, Table,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
import uuid
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import datetime
from datetime import date
import json
from fastapi import APIRouter
from typing import List
import pandas as pd
import requests
from openpyxl import load_workbook
from io import BytesIO
import logging

import settings
from models.models import AccessCredentials,MappedVariables, DataDictionaryTerms, DataDictionaries
from database import database
from serializers.dictionary_mapper_serializer import indicator_selector_entity,indicator_selector_list_entity
from serializers.access_credentials_serializer import access_credential_list_entity
from serializers.data_dictionary_serializer import data_dictionary_list_entity
from settings import settings


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)




router = APIRouter()


# # Create an inspector object to inspect the database
engine = None
inspector = None

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def createEngine():
    global engine, inspector, metadata
    try:
        credentials = AccessCredentials.objects().all()
        if credentials:
            credentials = access_credential_list_entity(credentials)
            connection_string = credentials
            # engine = create_engine(connection_string[0]["conn_string"])
            engine = create_engine(connection_string[0]["conn_string"])

            inspector = inspect(engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)

    except SQLAlchemyError as e:
        # Log the error or handle it as needed
        raise HTTPException(status_code=500, detail="Database connection error"+str(e))


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
    else:
        raise HTTPException(status_code=500, detail="Failed to initialize database engine")


# @router.get('/base_schemas')
# async def base_schemas():
#
#     # Check if download was successful
#     try:
#
#         wb = load_workbook('configs/data_dictionary/dictionary.xlsx')
#         sheets = wb.sheetnames
#         print('sheets ', sheets)
#         cass_session = database.cassandra_session_factory()
#
#         schemas = []
#
#         for schema in sheets:
#             print('schema --> ', schema)
#
#             df = pd.read_excel('configs/data_dictionary/dictionary.xlsx', sheet_name=schema)
#             base_variables = []
#             for i in range(0, df.shape[0]):
#                 query = "SELECT * FROM mapped_variables WHERE base_variable_mapped_to='%s' and base_repository='%s' ALLOW FILTERING;"%(df['Column/Variable Name'][i], schema)
#                 # results = database.execute_query(query)
#                 rows= cass_session.execute(query)
#                 results = []
#                 for row in rows:
#                     results.append(row)
#                 matchedVariable = False if not results else True
#
#                 base_variables.append({'variable':df['Column/Variable Name'][i], 'matched':matchedVariable})
#
#             baseSchemaObj = {}
#             baseSchemaObj["schema"] = schema
#             baseSchemaObj["base_variables"] = base_variables
#
#             schemas.append(baseSchemaObj)
#         return schemas
#     except Exception as e:
#         log.error('System ran into an error --> ', e)
#         return e

@router.get('/base_schemas')
async def base_schemas():
    try:
        schemas = DataDictionaries.objects().all()
        schemas =data_dictionary_list_entity(schemas)
        return schemas
    except Exception as e:
        log.error('System ran into an error --> ', e)
        return e


@router.get('/base_schema_variables/{base_lookup}')
async def base_variables(base_lookup: str):
    try:

        cass_session = database.cassandra_session_factory()

        schemas = []

        # df = pd.read_excel('configs/data_dictionary/dictionary.xlsx', sheet_name=base_lookup)
        query = "SELECT * FROM data_dictionary_terms WHERE dictionary='%s' ALLOW FILTERING;" % (base_lookup)
        dictionary = cass_session.execute(query)
        base_variables = []
        for i in dictionary:
            query = "SELECT * FROM mapped_variables WHERE base_variable_mapped_to='%s' and base_repository='%s' ALLOW FILTERING;"%(i['term'], base_lookup)
            rows= cass_session.execute(query)
            results = []
            for row in rows:
                results.append(row)
            matchedVariable = False if not results else True

            base_variables.append({'variable':i['term'], 'matched':matchedVariable})

        baseSchemaObj = {}
        baseSchemaObj["schema"] = base_lookup
        baseSchemaObj["base_variables"] = base_variables

        schemas.append(baseSchemaObj)
        return schemas
    except Exception as e:
        log.error('System ran into an error fetching base_schema_variables --->', e)
        return e
# @router.get('/base_schema_variables/{base_lookup}')
# async def base_variables(base_lookup: str):
#     try:
#
#         cass_session = database.cassandra_session_factory()
#
#         schemas = []
#
#         df = pd.read_excel('configs/data_dictionary/dictionary.xlsx', sheet_name=base_lookup)
#         base_variables = []
#         for i in range(0, df.shape[0]):
#             query = "SELECT * FROM mapped_variables WHERE base_variable_mapped_to='%s' and base_repository='%s' ALLOW FILTERING;"%(df['Column/Variable Name'][i], base_lookup)
#             rows= cass_session.execute(query)
#             results = []
#             for row in rows:
#                 results.append(row)
#             matchedVariable = False if not results else True
#
#             base_variables.append({'variable':df['Column/Variable Name'][i], 'matched':matchedVariable})
#
#         baseSchemaObj = {}
#         baseSchemaObj["schema"] = base_lookup
#         baseSchemaObj["base_variables"] = base_variables
#
#         schemas.append(baseSchemaObj)
#         return schemas
#     except Exception as e:
#         log.error('System ran into an error fetching base_schema_variables --->', e)
#         return e


@router.get('/base_variables/{base_lookup}')
async def base_variables(base_lookup: str):

    try:
        cass_session = database.cassandra_session_factory()

        query = "SELECT * FROM data_dictionary_terms WHERE dictionary='%s' ALLOW FILTERING;" % (base_lookup)
        dictionary = cass_session.execute(query)
        base_variables = []
        for i in dictionary:
            base_variables.append({"term":i['term'], "datatype":i['data_type']})
        return base_variables
    except Exception as e:
        log.error('System ran into an error fetching base_variables --->', e)
        return e


@router.get('/get_database_columns')
async def get_database_columns():
    try:
        dbTablesAndColumns={}

        table_names = metadata.tables.keys()

        for table_name in table_names:
            # Load the table schema from MetaData
            table = Table(table_name, metadata, autoload_with=engine)

            # Print column names and types
            getcolumnnames = []
            for column in table.columns:
                getcolumnnames.append({"name": column.name, "type": str(column.type)})
            # columns = inspector.get_columns(table_name)

            # getcolumnnames = []
            # for column in columns:
            #
            #     datatype=column['type']
            #     getcolumnnames.append({"Column": {column.name}, "Type": {column.type}})
            #     # getcolumnnames.append({"name":column['name'], "type":column['type']})

            dbTablesAndColumns[table_name] = getcolumnnames
        # credential = credential
        # print("dbTablesAndColumns =======>",dbTablesAndColumns)
        return dbTablesAndColumns
    except SQLAlchemyError as e:
        log.error('Error reflecting database: --->', e)



@router.post('/add_mapped_variables/{baselookup}')
async def add_mapped_variables(baselookup:str, variables:List[object]):
    try:
        #delete existing configs for base repo
        MappedVariables.objects(base_repository=baselookup).delete()
        for variableSet in variables:
            MappedVariables.create(tablename=variableSet["tablename"],columnname=variableSet["columnname"],
                                                   datatype=variableSet["datatype"], base_repository=variableSet["base_repository"],
                                                   base_variable_mapped_to=variableSet["base_variable_mapped_to"], join_by=variableSet["join_by"])
        return {"status":200, "message":"Mapped Variables added"}
    except Exception as e:
        return {"status":500, "message":e}


# @router.get('/tx_curr_variables')
# async def available_connections():
#     variables = MappedVariables.objects().all()
#     variables = indicator_selector_list_entity(variables)
#
#     # print(credentials)
#     return {'variables': variables}



@router.get('/generate_config')
async def generate_config(baseSchema:str):
    try:
        cass_session = database.cassandra_session_factory()

        query = "SELECT *  FROM mapped_variables WHERE base_repository='%s' ALLOW FILTERING;" % (baseSchema)
        config = cass_session.execute(query)

        results = []
        for row in config:
            results.append(indicator_selector_entity(row))

        with open('configs/schemas/'+baseSchema +'.conf', 'w') as f:
            f.write(str(results))

        return 'success'
    except Exception as e:
        log.error("Error generating config ==> %s", str(e))

        return e




@router.get('/import_config')
async def import_config(baseSchema:str):
    try:
        # delete existing configs for base repo
        cass_session = database.cassandra_session_factory()

        query = "SELECT *  FROM mapped_variables WHERE base_repository='%s' ALLOW FILTERING;" % (baseSchema)
        existingVariables = cass_session.execute(query)
        print('existingVariables ---> ',existingVariables)
        for var in existingVariables:
            print('delete before ---> ', var, var["id"])
            MappedVariables.objects(id=var["id"]).delete()
            print('delete after ---> ', var)


        f = open('configs/schemas/'+baseSchema +'.conf', 'r')

        configImportStatements = f.read()
        configs = json.loads(configImportStatements.replace("'", '"'))
        for conf in configs:
            print("print conf imported -->", conf)
            MappedVariables.create(tablename=conf["tablename"], columnname=conf["columnname"],
                                      datatype=conf["datatype"], base_repository=conf["base_repository"],
                                      base_variable_mapped_to=conf["base_variable_mapped_to"], join_by=conf["join_by"])

        f.close()
        log.info("========= Successfully imported config ==========")

        return 'success'
    except Exception as e:
        log.error("Error importing config ==> %s", str(e))

        return e


# @router.get('/generate-query/{baselookup}')
def generate_query(baselookup:str):
    try:

        cass_session = database.cassandra_session_factory()

        query = "SELECT * FROM mapped_variables WHERE base_repository='%s' ALLOW FILTERING;" % (baselookup)
        configs = cass_session.execute(query)

        primaryTable = MappedVariables.objects.filter(base_repository=baselookup, base_variable_mapped_to='PrimaryTableId').allow_filtering().first()
        primaryTableDetails = indicator_selector_entity(primaryTable)

        print("primaryTableDetails ---->", primaryTableDetails)

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


        query = f"SELECT {columns} from {primaryTableDetails['tablename']} {joins.replace(',','')} limit 10"

        log.info("========= Successfully generated query ==========")
        print("========= Successfully generated query result==========", query)
        return query
    except Exception as e:
        log.error("Error importing config ==> %s", str(e))

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


@router.get('/load_data/{baselookup}')
async def load_data(baselookup:str, db_session: Session = Depends(get_db)):
    try:

        query = text(generate_query(baselookup))

        cass_session = database.cassandra_session_factory()

        with db_session as session:
            result = session.execute(query)
            print("result --->", result)

            columns=result.keys()
            baseRepoLoaded = [dict(zip(columns,row)) for row in result]
            print("baseRepoLoaded --->", baseRepoLoaded)

            processed_results = [convert_datetime_to_iso(convert_none_to_null(result)) for result in baseRepoLoaded]
            print("processed_results --->", processed_results)
            # rows = []
            # Create a batch statement
            batch = BatchStatement()
            cass_session.execute("TRUNCATE TABLE %s;" %(baselookup))
            for data in processed_results:

                # rowvalues = data.values()
                # valuedata.extend(['NULL' if value is None else f"'{value}'" for value in data.values()])

                rowvalues = data.values()

                quoted_values = [
                    'NULL' if value is None
                    else f"'{value}'" if isinstance(value, str)
                    else f"'{value.strftime('%Y-%m-%d')}'" if isinstance(value, datetime.date)  # Convert date to string
                    else str(value)
                    for value in rowvalues
                ]
                print('quoted_values -->',quoted_values)

                query = f"""
                           INSERT INTO {baselookup} (client_repository_id, {", ".join(tuple(data.keys()))})
                           VALUES (uuid(), {', '.join(quoted_values)})
                       """
                print('insert query -->',query)
                # insert_resords = cass_session.prepare(query)
                cass_session.execute(query)
                # Add multiple insert statements to the batch
                # batch.add(query)
            # cass_session.execute(batch)

            # end batch
            cass_session.cluster.shutdown()
            return baseRepoLoaded
    except Exception as e:
        log.error("Error loading data ==> %s", str(e))

        return e


@router.get('/send/usl/{baselookup}')
async def send_data(baselookup:str, background_tasks: BackgroundTasks, db_session: Session = Depends(get_db)):
    try:

        query = query = f"""
                           SELECT * FROM {baselookup}
                       """

        cass_session = database.cassandra_session_factory()
        select_statement = query
        print("query ----> ", select_statement)
        result = cass_session.execute(select_statement)
        print('result to send ', result)

        # columns=result.column_names
        # baseRepoLoaded = [row for row in json.dumps(result)]
        baseRepoLoaded = [{key: (str(value) if isinstance(value, uuid.UUID)
                                else value.strftime('%Y-%m-%d') if isinstance(value,datetime.date)  # Convert date to string
                                else value) for key, value in
                            row.items()} for row in result]

        # print('staging to send ', settings.STAGING_API+baselookup)
        print('baseRepoLoaded to send ', baseRepoLoaded)

        cass_session.cluster.shutdown()
        data = {"facility":"BOMU facility",
                "facility_id":baseRepoLoaded[0]["facilityid"],
                "data":baseRepoLoaded
                }
        res = requests.post(settings.STAGING_API+baselookup,
                            json=data)
        print("STAGING_API data--->",res.status_code)

        background_tasks.add_task(simulate_long_task)
        # for progress in range(0, 101, 10):
        #     time.sleep(1)
        #     await send_progress_update(progress)

        return res.status_code
    except Exception as e:
        log.error("Error sending data ==> %s", str(e))

        return HTTPException(status_code=500,  detail=e)
    except BaseException as be:
        log.error("BaseException: Error sending data ==> %s", str(be))

        return HTTPException(status_code=500,  detail=be)

async def simulate_long_task():
    for progress in range(0, 101, 10):
        # Simulate a time-consuming task
        time.sleep(1)
        await send_progress_update(progress)
        print("simulate_long_task ,", progress)


@router.get('/manifest/usl/{baselookup}')
async def manifest(baselookup:str, db_session: Session = Depends(get_db)):
    try:
        cass_session = database.cassandra_session_factory()

        count_query = query = f"""
                           SELECT count(*) FROM {baselookup}
                       """
        count = cass_session.execute(count_query)

        columns_query = f"""
                SELECT column_name FROM system_schema.columns
                WHERE keyspace_name = 'datamap' AND table_name = '{baselookup}';
                """
        columns = cass_session.execute(columns_query)

        source_system_query = f"""
                        SELECT * FROM access_credentials
                        WHERE is_active = true ALLOW FILTERING;
                        """
        source_system = cass_session.execute(source_system_query)
        source_system = access_credential_list_entity(source_system)
        print("source_system ==>",source_system, source_system[0]['system'])

        manifest = {
            "usl_repository_name": baselookup,
            "count": [row['count'] for row in count][0],
            "columns": [row['column_name'] for row in columns],
            "session_id": uuid.uuid4(),
            "source_system_name": source_system[0]['system'],
            "source_system_version": source_system[0]['system_version'],
            "opendive_version": "1.0.0",
            "facility": "BOMU facility"
        }
        print("manifest ==>",manifest)

        cass_session.cluster.shutdown()
        print("manifest -->",manifest)
        return manifest
    except Exception as e:
        log.error("Error sending data ==> %s", str(e))

        return HTTPException(status_code=500,  detail=e)
    except BaseException as be:
        log.error("BaseException: Error sending data ==> %s", str(be))

        return HTTPException(status_code=500,  detail=be)



# Dictionary to store WebSocket connections and their associated progress
connections = {}

@router.websocket("/ws/progress")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    # Store the connection to send progress updates later
    connections[websocket] = 0
    print("websocket connections ==>", connections)

    try:
        while True:
            # Keep the connection open to send updates
            await websocket.receive_text()
            print("websocket ==>", websocket)
    except:
        # Clean up the WebSocket connection on error or disconnect
        del connections[websocket]
        await websocket.close()
        print("clean up websocket ==>", websocket)


async def send_progress_update(progress):
    # Send the progress update to all connected WebSocket clients
    print("connections list ? ==>", list(connections))

    for websocket in list(connections.keys()):
        try:
            await websocket.send_json({"progress": progress})
            print("progress updating ? ==>",progress, " list ==>", list(connections.keys()))
        except:
            del connections[websocket]  # Remove if the client has disconnected