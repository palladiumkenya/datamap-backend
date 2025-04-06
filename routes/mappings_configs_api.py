import json
from fastapi import APIRouter, HTTPException, Depends

import logging

from sqlalchemy.orm import Session

import settings
from database.database import get_db
from models.models import AccessCredentials, MappedVariables, ExtractsQueries
from serializers.dictionary_mapper_serializer import mapped_variable_entity, mapped_variable_list_entity

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

router = APIRouter()


@router.get('/generate_config/{baselookup}')
async def generate_config(baselookup: str, db: Session = Depends(get_db)):
    try:
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()

        # get mappings
        configs = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.source_system_id == source_system.id
        ).all()
        configs = mapped_variable_list_entity(configs)

        # get query
        existingQuery = db.query(ExtractsQueries).filter(
            ExtractsQueries.base_repository == baselookup,
            ExtractsQueries.source_system_id == source_system.id
        ).first()

        mappings = []
        for row in configs:
            mappings.append(row)

        generatedConfig = {
            "base_repository": existingQuery.base_repository,
            "query": existingQuery.query,
            "mappings": mappings
        }

        with open('configs/schemas/' + baselookup + '.conf', 'w') as f:
            f.write(str(generatedConfig).replace("'", '"'))

        log.info(f'+++++++++++ Successfully uploaded config: {baselookup}+++++++++++')
        return 'success'
    except Exception as e:
        log.error("Error generating config ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error uploadiing mappings to marketplace:" + str(e))


@router.get('/import_config/{baselookup}')
async def import_config(baselookup: str, db: Session = Depends(get_db)):
    try:
        # clear existing mappings
        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()
        existingMappings = db.query(MappedVariables).filter(
            MappedVariables.base_repository == baselookup,
            MappedVariables.source_system_id == source_system.id
        ).delete()
        db.commit()

        f = open('configs/schemas/' + baselookup + '.conf', 'r')

        configImportStatements = f.read()
        # configs = json.loads(configImportStatements.replace("'", '"'))

        configs = json.loads(configImportStatements)

        # add the mappings
        for conf in configs["mappings"]:
            MappedVariables(tablename=conf["tablename"], columnname=conf["columnname"],
                            datatype=conf["datatype"], base_repository=conf["base_repository"],
                            base_variable_mapped_to=conf["base_variable_mapped_to"],
                            join_by=conf["join_by"], source_system_id=source_system.id)

        # add the query
        existingQuery = db.query(ExtractsQueries).filter(
            ExtractsQueries.base_repository==baselookup,
            ExtractsQueries.source_system_id==source_system.id
        ).first()

        if existingQuery:
            queries = db.query(ExtractsQueries).filter(ExtractsQueries.id==existingQuery.id).first()
            queries.query = configs["query"]
        else:
            new_query = ExtractsQueries(
                query=configs["query"],
                base_repository=configs["base_repository"],
                source_system_id=source_system.id
            )
            db.add(new_query)
        db.commit()

        f.close()
        log.info("+++++++++ Successfully imported config ++++++++++")

        return 'success'
    except Exception as e:
        log.error("Error importing config ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error importing mappings to system system:" + str(e))
