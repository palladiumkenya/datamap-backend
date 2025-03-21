
from pydantic import BaseModel
import uuid

import json
from fastapi import APIRouter,HTTPException
from typing import List

import logging

import settings
from models.models import AccessCredentials,MappedVariables, DataDictionaryTerms, DataDictionaries,SiteConfig,\
    TransmissionHistory, ExtractsQueries
from database import database
from serializers.dictionary_mapper_serializer import mapped_variable_entity,mapped_variable_list_entity



log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)




router = APIRouter()




@router.get('/generate_config/{baselookup}')
async def generate_config(baselookup :str):
    try:
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()

        # get mappings
        configs = MappedVariables.objects.filter(base_repository=baselookup,
                                                 source_system_id=source_system['id']).allow_filtering()
        configs = mapped_variable_list_entity(configs)

        # get query
        existingQuery = ExtractsQueries.objects(base_repository=baselookup,
                                                source_system_id=source_system['id']).allow_filtering().first()

        mappings = []
        for row in configs:
            mappings.append(row)

        generatedConfig = {
            "base_repository" :existingQuery["base_repository"],
            "query" :existingQuery["query"],
            "mappings" :mappings
        }

        with open('configs/schemas/' +baselookup +'.conf', 'w') as f:
            f.write(str(generatedConfig).replace("'", '"'))

        log.info(f'+++++++++++ Successfully uploaded config: {baselookup}+++++++++++')
        return 'success'
    except Exception as e:
        log.error("Error generating config ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error uploadiing mappings to marketplace:" + str(e))


@router.get('/import_config/{baselookup}')
async def import_config(baselookup: str):
    try:
        # clear existing mappings
        source_system = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
        existingMappings = MappedVariables.objects(base_repository=baselookup,
                                                   source_system_id=source_system['id']).allow_filtering().all()
        for mapping in existingMappings:
            mapping.delete()

        f = open('configs/schemas/' + baselookup + '.conf', 'r')

        configImportStatements = f.read()
        # configs = json.loads(configImportStatements.replace("'", '"'))

        configs = json.loads(configImportStatements)

        # add the mappings
        for conf in configs["mappings"]:
            MappedVariables.create(tablename=conf["tablename"], columnname=conf["columnname"],
                                   datatype=conf["datatype"], base_repository=conf["base_repository"],
                                   base_variable_mapped_to=conf["base_variable_mapped_to"],
                                   join_by=conf["join_by"], source_system_id=source_system['id'])

        # add the query
        existingQuery = ExtractsQueries.objects(base_repository=baselookup,
                                                source_system_id=source_system['id']).allow_filtering().first()

        if existingQuery:
            ExtractsQueries.objects(id=existingQuery["id"]).update(
                query=configs["query"]
            )
        else:
            ExtractsQueries.create(query=configs["query"],
                                   base_repository=configs["base_repository"],
                                   source_system_id=source_system['id'])

        f.close()
        log.info("+++++++++ Successfully imported config ++++++++++")

        return 'success'
    except Exception as e:
        log.error("Error importing config ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error importing mappings to system system:" + str(e))

