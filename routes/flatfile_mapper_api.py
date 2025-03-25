from sqlalchemy import create_engine, inspect, MetaData, Table,text

import json
from fastapi import Depends, APIRouter,HTTPException
from typing import List

import logging

import settings
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
async def get_columns(conn_type:str, cass_session = Depends(database.cassandra_session_factory)):
    credentials = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
    if credentials:
        try:
            tableName = f"{credentials['name'].lower()}_{conn_type}_extract"
            query = f"SELECT column_name FROM system_schema.columns WHERE keyspace_name='datamap' AND " \
                    f"table_name='{tableName}'"
            rows = cass_session.execute(query)
            dbTablesAndColumns = [row["column_name"] for row in rows]

            return {"data":dbTablesAndColumns}
        except Exception as e:
            log.error('Error getting csv columns: --->', e)
            raise HTTPException(status_code=500, detail='Error reflecting source database')
    else:
        log.error('Error getting csv columns: --->')
        raise HTTPException(status_code=500, detail='Error getting csv columns')


@router.post('/add/{conn_type}/mapped_variables/{baselookup}')
async def add_mapped_variables(conn_type:str, baselookup:str, variables:List[object]):
    try:
        #delete existing configs for base repo
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        existingMappings = MappedVariables.objects(base_repository=baselookup,source_system_id=source_system['id']).allow_filtering().all()
        for mapping in existingMappings:
            mapping.delete()

        credentials = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()

        for variableSet in variables:
            MappedVariables.create(tablename=f"{credentials['name'].lower()}_{conn_type}_extract",
                                   columnname=variableSet["columnname"],
                                   datatype="-", base_repository=baselookup,
                                   base_variable_mapped_to=variableSet["base_variable_mapped_to"],
                                   join_by="-", source_system_id=source_system['id'])

        # after saving mappings, generate query from them and save
        extract_source_data_query = generate_flatfile_query(baselookup)

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


@router.post('/test/{conn_type}/mapped_variables/{baselookup}')
async def test_mapped_variables(conn_type:str, baselookup:str, variables:List[object], cass_session = Depends(database.cassandra_session_factory)):
    try:
        extractQuery = generate_test_query(conn_type, variables)

        with cass_session as session:
            baseRepoLoaded = session.execute(extractQuery)

            processed_results = [result for result in baseRepoLoaded]

            # check if base variable terms are all in the columns provided in the custom query
            list_of_issues = validateMandatoryFields(baselookup, variables, processed_results)

        return {"data":list_of_issues}
    except Exception as e:
        # return {"status":500, "message":e
        raise HTTPException(status_code=500, detail="Error testing mappings on source system:" + str(e))



def generate_flatfile_query(baselookup:str):
    try:
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()

        configs = MappedVariables.objects.filter(base_repository=baselookup,source_system_id=source_system['id']).allow_filtering()
        configs = mapped_variable_list_entity(configs)

        mapped_columns = []
        mapped_joins = []

        for conf in configs:
            if conf["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(conf["columnname"] +" as "+conf["base_variable_mapped_to"]+" ")

        columns = ", ".join(mapped_columns)

        site_config = SiteConfig.objects.filter(is_active=True).allow_filtering().first()
        mappedSiteCode = MappedVariables.objects.filter(base_repository=baselookup, base_variable_mapped_to='FacilityID',
                                                        source_system_id=source_system['id']).allow_filtering().first()

        tableName= configs[0]["tablename"]
        query = f"SELECT {columns} from {tableName} "
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e



def generate_test_query(conn_type:str, variableSet:List[object]):
    try:
        credentials = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()

        tableName = f"{credentials['name'].lower()}_{conn_type}_extract"

        mapped_columns = []

        for variableMapped in variableSet:
            if variableMapped["base_variable_mapped_to"] != 'PrimaryTableId':
                mapped_columns.append(variableMapped["columnname"] +" as \""+variableMapped["base_variable_mapped_to"]+"\" ")

        columns = ", ".join(mapped_columns)

        query = f"SELECT {columns} from {tableName} "
        log.info("++++++++++ Successfully generated query +++++++++++")
        return query
    except Exception as e:
        log.error("Error generating query. ERROR: ==> %s", str(e))

        return e