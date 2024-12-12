import re

from fastapi import  Depends, HTTPException
from sqlalchemy import create_engine, inspect, MetaData, Table,text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from cassandra.query import BatchStatement
import datetime
# from datetime import datetime

import json
from fastapi import APIRouter
from typing import List

import logging

import settings
from models.models import AccessCredentials,MappedVariables, DataDictionaryTerms, DataDictionaries,SiteConfig,TransmissionHistory
from database import database
from serializers.dictionary_mapper_serializer import mapped_variable_entity,mapped_variable_list_entity
from serializers.data_dictionary_serializer import data_dictionary_list_entity, data_dictionary_terms_list_entity
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
metadata = None
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def createEngine():
    global engine, inspector, metadata
    log.info('===== start creating an engine =====')

    try:
        credentials = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        if credentials:
            # credentials = access_credential_list_entity(credentials)
            connection_string = credentials
            # engine = create_engine(connection_string[0]["conn_string"])
            engine = create_engine(connection_string["conn_string"])

            inspector = inspect(engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            log.info('===== Database reflected ====')

    except SQLAlchemyError as e:
        # Log the error or handle it as needed
        log.error('===== Database not reflected ==== ERROR:', str(e))
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
        dictionary = DataDictionaryTerms.objects.filter(dictionary=baselookup).allow_filtering()
        dictionary = data_dictionary_terms_list_entity(dictionary)

        schemas = []

        base_variables = []
        for i in dictionary:
            configs = MappedVariables.objects.filter(base_variable_mapped_to=i['term'],base_repository=baselookup).allow_filtering()

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
            base_variables.append({"term":term.term, "datatype":term.data_type})

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
        # MappedVariables.objects(base_repository=baselookup).delete()
        existingMappings = MappedVariables.objects(base_repository=baselookup).all()
        for mapping in existingMappings:
            mapping.delete()

        for variableSet in variables:
            MappedVariables.create(tablename=variableSet["tablename"],columnname=variableSet["columnname"],
                                                   datatype=variableSet["datatype"], base_repository=variableSet["base_repository"],
                                                   base_variable_mapped_to=variableSet["base_variable_mapped_to"], join_by=variableSet["join_by"])
        return {"status":200, "message":"Mapped Variables added"}
    except Exception as e:
        return {"status":500, "message":e}


@router.post('/test/mapped_variables/{baselookup}')
async def test_mapped_variables(baselookup:str, variables:List[object], db_session: Session = Depends(get_db)):
    try:
        extractQuery = text(generate_query(baselookup))

        with db_session as session:
            result = session.execute(extractQuery)

            columns = result.keys()
            baseRepoLoaded = [dict(zip(columns, row)) for row in result]

            processed_results = [result for result in baseRepoLoaded]

        list_of_issues = validateMandatoryFields(baselookup, variables, processed_results)

        return {"data":list_of_issues}
    except Exception as e:
        return {"status":500, "message":e}


def validateMandatoryFields(baselookup:str, variables:List[object], processed_results:List[object]):

    list_of_issues = []
    for variableSet in variables:
        if variableSet["base_variable_mapped_to"] != "PrimaryTableId":
            filteredData = [obj[variableSet["base_variable_mapped_to"]] for obj in processed_results]

            dictTerms = DataDictionaryTerms.objects.filter(dictionary=baselookup, term=variableSet["base_variable_mapped_to"]).allow_filtering().first()

            if dictTerms["is_required"] == True:
                if "" in filteredData or None in filteredData or "NULL" in filteredData:
                    issueObj = {"base_variable": variableSet["base_variable_mapped_to"],
                                "issue": "*Variable is Mandatory. Data is expected in all records.",
                                "column_mapped": variableSet["columnname"],
                                "recommended_solution": "Ensure all records have this data"}
                    list_of_issues.append(issueObj)
    return list_of_issues


@router.get('/generate_config/{baselookup}')
async def generate_config(baselookup:str):
    try:
        configs = MappedVariables.objects.filter(base_repository=baselookup).allow_filtering()
        configs = mapped_variable_list_entity(configs)

        results = []
        for row in configs:
            results.append(row)

        with open('configs/schemas/'+baselookup +'.conf', 'w') as f:
            f.write(str(results))

        log.info(f'+++++++++++ Successfully uploaded config: {baselookup}+++++++++++')
        return 'success'
    except Exception as e:
        log.error("Error generating config ==> %s", str(e))

        return e




@router.get('/import_config/{baselookup}')
async def import_config(baselookup:str):
    try:
        # delete existing configs for base repo
        cass_session = database.cassandra_session_factory()

        query = "SELECT *  FROM mapped_variables WHERE base_repository='%s' ALLOW FILTERING;" % (baselookup)
        existingVariables = cass_session.execute(query)

        for var in existingVariables:
            MappedVariables.objects(id=var["id"]).delete()

        f = open('configs/schemas/'+baselookup +'.conf', 'r')

        configImportStatements = f.read()
        configs = json.loads(configImportStatements.replace("'", '"'))
        for conf in configs:
            MappedVariables.create(tablename=conf["tablename"], columnname=conf["columnname"],
                                      datatype=conf["datatype"], base_repository=conf["base_repository"],
                                      base_variable_mapped_to=conf["base_variable_mapped_to"], join_by=conf["join_by"])

        f.close()
        log.info("+++++++++ Successfully imported config ++++++++++")

        return 'success'
    except Exception as e:
        log.error("Error importing config ==> %s", str(e))

        return e


# @router.get('/generate-query/{baselookup}')
def generate_query(baselookup:str):
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


        query = f"SELECT {columns} from {primaryTableDetails['tablename']} {joins.replace(',','')}"

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


@router.get('/load_data/{baselookup}')
async def load_data(baselookup:str, db_session: Session = Depends(get_db)):
    try:
        query = text(generate_query(baselookup))

        cass_session = database.cassandra_session_factory()

        # started loading
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        site_config = SiteConfig.objects.filter(is_active=True).allow_filtering().first()
        loadedHistory = TransmissionHistory(usl_repository_name=baselookup, action="Loading",
                            facility=f'{site_config["site_name"]}-{site_config["site_code"]}',
                            source_system_id=source_system['id'],
                            source_system_name=site_config['primary_system'],
                            ended_at=None,
                            manifest_id=None).save()

        with db_session as session:
            result = session.execute(query)

            columns = result.keys()
            baseRepoLoaded = [dict(zip(columns,row)) for row in result]

            # processed_results = [convert_datetime_to_iso(convert_none_to_null(result)) for result in baseRepoLoaded]
            processed_results=[result for result in baseRepoLoaded]

            batch = BatchStatement()
            cass_session.execute("TRUNCATE TABLE %s;" %(baselookup))
            for data in processed_results:
                quoted_values = [
                    'NULL' if value is None
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
                log.info("+++++++ data +++++++")

                cass_session.execute(query)
                # Add multiple insert statements to the batch
                # batch.add(query)
            # cass_session.execute(batch)
            log.info("+++++++ batch saved +++++++")

            # end batch
            cass_session.cluster.shutdown()

            # ended loading
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
