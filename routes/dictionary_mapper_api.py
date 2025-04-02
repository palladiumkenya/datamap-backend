import uuid

from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, inspect, MetaData, Table, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
import datetime
from contextlib import contextmanager

import json
from fastapi import APIRouter
from typing import List

import logging

from database.database import execute_data_query, get_db as get_main_db, execute_query
from models.models import AccessCredentials, MappedVariables, DataDictionaryTerms, DataDictionaries, SiteConfig, \
    TransmissionHistory
from serializers.dictionary_mapper_serializer import mapped_variable_entity, mapped_variable_list_entity
from serializers.data_dictionary_serializer import data_dictionary_list_entity, data_dictionary_terms_list_entity

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
        query = text("""
            SELECT * FROM access_credentials WHERE is_active = True LIMIT 1
        """)
        credentials = execute_data_query(query)
        if credentials and credentials[0].conn_type != "csv":
            connection_string = credentials[0]
            engine = create_engine(connection_string.conn_string)

            inspector = inspect(engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            log.info('===== Database reflected ====')

    except SQLAlchemyError as e:
        # Log the error or handle it as needed
        log.error('===== Database not reflected ==== ERROR:', str(e))
        raise HTTPException(status_code=500, detail="Database connection error" + str(e)) from e


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
async def base_schemas(db: Session = Depends(get_main_db)):
    try:
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
async def get_database_columns():
    if metadata:
        try:
            dbTablesAndColumns = {}

            table_names = metadata.tables.keys()

            for table_name in table_names:
                # Load the table schema from MetaData
                table = Table(table_name, metadata, autoload_with=engine)

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


@router.get('/get_csv_columns')
async def get_csv_columns(db: Session = Depends(get_main_db)):
    credentials = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()
    if credentials and credentials.conn_type == "csv":
        try:
            query = text(f"""SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name='{credentials.name.lower()}_csv_extract'""")
            rows = execute_data_query(query)
            dbTablesAndColumns = [row.column_name for row in rows]

            return dbTablesAndColumns
        except Exception as e:
            log.error('Error getting csv columns: --->', e)
            raise HTTPException(status_code=500, detail='Error reflecting source database')
    else:
        log.error('Error getting csv columns: --->')
        raise HTTPException(status_code=500, detail='Error getting csv columns')


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

        return {"status": 200, "message": "Mapped Variables added"}
    except Exception as e:
        print(str(e))
        return {"status": 500, "message": e}


@router.post('/test/mapped_variables/{baselookup}')
async def test_mapped_variables(baselookup: str, variables: List[object], db_session: Session = Depends(get_db),
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
                if "" in filteredData or None in filteredData or "NULL" in filteredData:
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
                mapped_columns.append(
                    variableMapped["tablename"] + "." + variableMapped["columnname"] + " as " + variableMapped[
                        "base_variable_mapped_to"] + " ")
                if all(variableMapped["tablename"] + "." not in s for s in mapped_joins):
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


@router.get('/generate_config/{baselookup}')
async def generate_config(baselookup: str, db: Session = Depends(get_main_db)):
    try:
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()

        configs = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.source_system_id == source_system.id
        ).all()
        configs = mapped_variable_list_entity(configs)

        with open('configs/schemas/' + baselookup + '.conf', 'w') as f:
            f.write(str(configs))

        log.info(f'+++++++++++ Successfully uploaded config: {baselookup}+++++++++++')
        return 'success'
    except Exception as e:
        log.error("Error generating config ==> %s", str(e))

        return e


@router.get('/import_config/{baselookup}')
async def import_config(baselookup: str, db: Session = Depends(get_main_db)):
    try:

        query = text(f"SELECT * FROM mapped_variables WHERE base_repository='{baselookup}'")
        existingVariables = execute_data_query(query)

        for var in existingVariables:
            db.query(MappedVariables).filter(MappedVariables.id == var["id"]).delete()

        f = open('configs/schemas/' + baselookup + '.conf', 'r')

        configImportStatements = f.read()
        configs = json.loads(configImportStatements.replace("'", '"'))
        for conf in configs:
            variables = MappedVariables(tablename=conf["tablename"], columnname=conf["columnname"],
                                        datatype=conf["datatype"], base_repository=conf["base_repository"],
                                        base_variable_mapped_to=conf["base_variable_mapped_to"],
                                        join_by=conf["join_by"])
            db.add(variables)
        db.commit()

        f.close()
        log.info("+++++++++ Successfully imported config ++++++++++")

        return 'success'
    except Exception as e:
        log.error("Error importing config ==> %s", str(e))

        return e


# @router.get('/generate-query/{baselookup}')
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


# @router.get('/load_data/{baselookup}')
async def load_data(baselookup: str, websocket: WebSocket, db):
    try:
        # cass_session = database.cassandra_session_factory()

        extract_source_data_query = text(generate_query(baselookup, db))
        source_data_count_query = text(source_total_count(baselookup, db))

        # started loading
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()
        site_config = db.query(SiteConfig).filter(SiteConfig.is_active == True).first()
        loadedHistory = TransmissionHistory(usl_repository_name=baselookup, action="Loading",
                                            facility=f'{site_config.site_name}-{site_config.site_code}',
                                            source_system_id=source_system.id,
                                            source_system_name=site_config.primary_system,
                                            ended_at=None,
                                            manifest_id=None)
        db.add(loadedHistory)
        db.commit()

        with get_db() as session:
            get_count = session.execute(source_data_count_query)
            source_count = get_count.scalar()

            result = session.execute(extract_source_data_query)

            columns = result.keys()
            baseRepoLoaded = [dict(zip(columns, row)) for row in result]

            processed_results = [result for result in baseRepoLoaded]

            execute_query(text(f"TRUNCATE TABLE {baselookup}"))
            # for data in processed_results:
            count_inserted = 0
            batch_size = 100
            for i in range(0, len(processed_results), batch_size):
                batch = processed_results[i:i + batch_size]
                for data in batch:
                    quoted_values = [
                        'NULL' if value is None
                        else f"'{value}'" if isinstance(value, str)
                        else f"'{value.strftime('%Y-%m-%d')}'" if isinstance(value, datetime.date)
                        else f"'{value}'" if ((db.query(DataDictionaryTerms).filter(
                            DataDictionaryTerms.dictionary == baselookup,
                            DataDictionaryTerms.term == key).first()).data_type == "NVARCHAR")
                        else str(value)
                        for key, value in data.items()
                    ]

                    idColumn = baselookup + "_id"

                    query = text(f"""
                               INSERT INTO {baselookup} ({idColumn}, {", ".join(tuple(data.keys()))})
                               VALUES ('{uuid.uuid4()}', {', '.join(quoted_values)})
                           """)

                    execute_query(query)
                    # add up records
                    count_inserted += 1

                # count_inserted = inserted_total_count(baselookup)
                await websocket.send_text(f"{count_inserted}")
                log.info("+++++++ data batch +++++++")
                log.info(f"+++++++ step i : count_inserted +++++++ {count_inserted} records")

            # cass_session.execute(batch)
            log.info("+++++++ USL Base Repository Data saved +++++++")

        baseRepoLoaded_json_data = json.dumps(baseRepoLoaded, default=str)

        # Send the JSON string over the WebSocket
        await websocket.send_text(baseRepoLoaded_json_data)
        await websocket.close()
        # ended loading
        # loadedHistory.ended_at=datetime.utcnow()
        # loadedHistory.save()
        # TransmissionHistory.objects(id=loadedHistory.id).update(ended_at=datetime.utcnow())

        return {"data": baseRepoLoaded}
    except Exception as e:
        log.error("Error loading data ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error loading data:" + str(e))


def source_total_count(baselookup: str, db):
    try:
        configs = db.query(MappedVariables).filter(MappedVariables.base_repository==baselookup).all()
        configs = mapped_variable_list_entity(configs)

        primaryTableDetails = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.base_variable_mapped_to == 'PrimaryTableId'
        ).first()

        mapped_columns = []
        mapped_joins = []

        for conf in configs:
            if conf['base_variable_mapped_to'] != 'PrimaryTableId':
                mapped_columns.append(
                    conf['tablename'] + "." + conf['columnname'] + " as \"" + conf['base_variable_mapped_to'] + "\" ")
                if all(conf['tablename'] + "." not in s for s in mapped_joins):
                    if conf['tablename'] != primaryTableDetails.tablename:
                        mapped_joins.append(" LEFT JOIN " + conf['tablename'] + " ON " + primaryTableDetails.tablename.strip()
                                            + "." + primaryTableDetails.columnname.strip() +
                                            " = " + conf['tablename'].strip() + "." + conf['join_by'].strip())

        columns = ", ".join(mapped_columns)
        joins = ", ".join(mapped_joins)

        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active==True).first()
        site_config = db.query(SiteConfig).filter(SiteConfig.is_active==True).first()
        mappedSiteCode = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.base_variable_mapped_to == 'FacilityID',
            MappedVariables.source_system_id == source_system.id
        ).first()

        query = f"""SELECT count(*) as count from {primaryTableDetails.tablename} 
        {joins.replace(',', '')}
        WHERE  CAST({mappedSiteCode.tablename}.{mappedSiteCode.columnname} AS INT) = {site_config.site_code}"""

        log.info("++++++++++ Successfully generated count query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e


def inserted_total_count(baselookup: str):
    try:
        totalRecordsquery = f"SELECT COUNT(*) count as count FROM {baselookup}"
        totalRecordsresult = execute_data_query(totalRecordsquery)

        insertedCount = totalRecordsresult[0].count
        print("insertedCount--->", insertedCount)

        return insertedCount
    except Exception as e:
        log.error("Error getting total inserted query. ERROR: ==> %s", str(e))
        return 0


@router.websocket("/ws/load/progress/{baselookup}")
async def progress_websocket_endpoint(
        baselookup: str, websocket: WebSocket, db_session: Session = Depends(get_db), db: Session = Depends(get_main_db)
):
    await websocket.accept()
    print("websocket manifest -->", baselookup)

    try:
        # count of data in source to be loaded
        query = text(source_total_count(baselookup, db))
        print("source query -->", query)

        with db_session as session:
            result = session.execute(query)
            sourceCount = result.scalar()
        print("source count -->", sourceCount)
        while True:
            data = await websocket.receive_text()
            baseRepo = data
            print("websocket manifest -->", baseRepo)
            await load_data(baselookup, websocket, db)
            # BackgroundTasks.add_task(inserted_total_count(baselookup,sourceCount,websocket))
    except WebSocketDisconnect:
        log.error("Client disconnected")
        await websocket.close()
    except Exception as e:
        log.error("Websocket error ==> %s", str(e))
        await websocket.close()
