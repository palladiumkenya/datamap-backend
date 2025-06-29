import uuid

from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, inspect, MetaData, Table, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
import datetime
from contextlib import contextmanager
from pydantic import BaseModel
import json
from fastapi import APIRouter
from typing import List

import logging

from database.database import execute_data_query, get_db as get_main_db, execute_query,execute_query_return_dict, engine as postgres_engine
from database.source_system_database import get_source_db, engine as source_db_engine

from models.models import AccessCredentials, MappedVariables, DataDictionaryTerms, DataDictionaries, SiteConfig, \
    TransmissionHistory, ExtractsQueries
from models import models
from serializers.dictionary_mapper_serializer import mapped_variable_entity, mapped_variable_list_entity
from serializers.data_dictionary_serializer import data_dictionary_list_entity, data_dictionary_terms_list_entity
from utils.dqa_check import dqa_check


class QueryModel(BaseModel):
    query: str


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

router = APIRouter()


async def load_data(baselookup: str, websocket: WebSocket, db):
    try:
        # system config data
        source_system = db.query(AccessCredentials).filter(
            AccessCredentials.is_active == True).first()
        site_config = db.query(SiteConfig).filter(
            SiteConfig.is_active == True).first()

        existingQuery = db.query(ExtractsQueries).filter(
            ExtractsQueries.base_repository==baselookup,
            ExtractsQueries.source_system_id==source_system.id
        ).first()
        extract_source_data_query = existingQuery.query

        # ------ started extraction -------

        loadedHistory = TransmissionHistory(usl_repository_name=baselookup, action="Loaded",
                                            facility=f'{site_config.site_name}-{site_config.site_code}',
                                            source_system_id=source_system.id,
                                            source_system_name=site_config.primary_system,
                                            ended_at=None,
                                            manifest_id=None)
        db.add(loadedHistory)
        db.commit()

        processed_results=[]
        if source_system.conn_type not in ["csv", "api"]:
            # extract data from source DB
            with get_source_db() as session:

                extractresults = session.execute(text(extract_source_data_query))
                result = extractresults.fetchall()
                print([row for row in result])
                columns = extractresults.keys()
                baseRepoLoaded = [dict(zip(columns,row)) for row in result]

                processed_results=[result for result in baseRepoLoaded]
        else:
            # extract data from imported csv/api schema
            baseRepoLoaded = execute_query_return_dict(text(extract_source_data_query))
            processed_results = [result for result in baseRepoLoaded]
            print(processed_results)
        # ------ --------------- -------
        # ------ started loading -------

        if len(processed_results) > 0:
            # clear base repo data in preparation for inserting new data
            execute_query(text(f"TRUNCATE TABLE {baselookup}"))

            count_inserted = 0
            batch_size = 300
            idColumn = baselookup.lower() + "_id"

            for i in range(0, len(processed_results), batch_size):
                batch = processed_results[i:i + batch_size]
                dataToBeInserted = []

                for data in batch:
                    for key, value in data.items():

                        quoted_values = [ None if value is None
                            else None if value == ''
                            else int(value) if ((db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary==baselookup, DataDictionaryTerms.term==key).first()).data_type == "INT")
                            else bool(value) if ((db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary==baselookup, DataDictionaryTerms.term==key).first()).data_type == "BOOLEAN")
                            else f"{convert_datetime_to_iso(value)}" if "DATETIME" in ((db.query(DataDictionaryTerms).filter(DataDictionaryTerms.dictionary==baselookup, DataDictionaryTerms.term==key).first()).data_type)
                            else f"{value}" if isinstance(value, str)
                            else f"{value}" if ((db.query(DataDictionaryTerms).filter(
                                DataDictionaryTerms.dictionary == baselookup,
                                DataDictionaryTerms.term == key).first()).data_type == "NVARCHAR")
                            else str(value)]
                        data[key] = quoted_values[0]
                    # dataToBeInserted.append(data)

                    # for db case sensitivity
                    newRecordObj = {}
                    newRecordObj[idColumn] = uuid.uuid4()
                    for key, val in data.items():
                        newRecordObj[key.lower()] = val
                    dataToBeInserted.append(newRecordObj)

                    count_inserted += 1

                postgres_metadata = MetaData()
                postgres_metadata.reflect(bind=postgres_engine)
                USLDictionaryModel = postgres_metadata.tables.get(baselookup.lower())

                if USLDictionaryModel is None:
                    print(f"Table {baselookup} does not exist in the database.")
                    return

                # new_records = [USLDictionaryModel(**data) for key, value in dataToBeInserted]
                insert_stmt = USLDictionaryModel.insert().values(dataToBeInserted)
                db.execute(insert_stmt)
                # db.add_all(new_records)
                db.commit()

                await websocket.send_text(f"{count_inserted}")
                log.info("+++++++ data batch +++++++")
                log.info(f"+++++++ step i : count_inserted +++++++ {count_inserted} records")

            log.info("+++++++ USL Base Repository Data saved +++++++")

        # end batch
        baseRepoLoaded_json_data = json.dumps(processed_results, default=str)

        # Send the JSON string over the WebSocket
        await websocket.send_text(baseRepoLoaded_json_data)
        await websocket.close()
        # ended loading
        # TODO - update history
        # loadedHistory.ended_at=datetime.utcnow()
        # loadedHistory.save()
        # TransmissionHistory.objects(id=loadedHistory.id).update(ended_at=datetime.utcnow())
        dqa_check(baselookup, db)

        return {"data": baseRepoLoaded}
    except Exception as e:
        error = json.dumps({"status_code":500, "message":e}, default=str)

        # Send the error over the WebSocket
        await websocket.send_text(error)
        await websocket.close()
        log.error("Error loading data ==> %s", str(e))
        raise HTTPException(status_code=500, detail="Error loading data:" + str(e))


@router.websocket("/ws/load/progress/{baselookup}")
async def progress_websocket_endpoint(
        baselookup: str, websocket: WebSocket, db: Session = Depends(get_main_db)
):
    await websocket.accept()
    print("websocket manifest -->", baselookup)

    try:

        while True:
            data = await websocket.receive_text()
            baseRepo = data
            print("websocket manifest -->", baseRepo)
            await load_data(baselookup, websocket, db)
    except WebSocketDisconnect:
        log.error("Client disconnected")
        await websocket.close()
    except Exception as e:
        log.error("Websocket error ==> %s", str(e))
        await websocket.close()


def convert_datetime_to_iso(value):
    # if isinstance(value, datetime.date):
    #     return value.strftime('%Y-%m-%d')
    # else:

    if isinstance(value, datetime.datetime):
        return value.date()
    elif isinstance(value, datetime.date):
        return value
    else:
        date_formats = ['%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d']
        for date_format in date_formats:
            try:
                date_object = datetime.datetime.strptime(value, date_format).date()
                return date_object
            except (ValueError, TypeError):
                continue








