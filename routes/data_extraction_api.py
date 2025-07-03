import uuid

from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, inspect, MetaData, Table, text, insert
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
from routes.dictionary_mapper_api import createEngine,get_engine_state


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
async def progress_websocket_endpoint(baselookup: str, websocket: WebSocket, db: Session = Depends(get_main_db)):
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



# def extract_data_pipeline():
#     try:
#         baselookup = "lab"
#
#         # Main DB (e.g. PostgreSQL or SQLite for app config)
#         db_gen = get_main_db()
#         db = next(db_gen)
#
#         # Fetch system configuration
#         source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()
#         site_config = db.query(SiteConfig).filter(SiteConfig.is_active == True).first()
#
#         existing_query = db.query(ExtractsQueries).filter(
#             ExtractsQueries.base_repository == baselookup,
#             ExtractsQueries.source_system_id == source_system.id
#         ).first()
#         extract_sql = existing_query.query
#
#         # Log start of extraction
#         loaded_history = TransmissionHistory(
#             usl_repository_name=baselookup,
#             action="Loaded",
#             facility=f'{site_config.site_name}-{site_config.site_code}',
#             source_system_id=source_system.id,
#             source_system_name=site_config.primary_system,
#             ended_at=None,
#             manifest_id=None
#         )
#         db.add(loaded_history)
#         db.commit()
#
#         # Extract data
#         processed_results = []
#         if source_system.conn_type not in ["csv", "api"]:
#             with get_source_db() as session:
#                 results = session.execute(text(extract_sql))
#                 rows = results.fetchall()
#                 columns = results.keys()
#                 processed_results = [dict(zip(columns, row)) for row in rows]
#         else:
#             processed_results = execute_query_return_dict(text(extract_sql))
#
#         if not processed_results:
#             log.warning("No records found to process.")
#             return
#
#         # Clear base table before insert
#         with postgres_engine.connect() as conn:
#             conn.execute(text(f"TRUNCATE TABLE {baselookup}"))
#             conn.commit()
#
#         # Prepare and insert in batches
#         count_inserted = 0
#         batch_size = 300
#         id_column = baselookup.lower() + "_id"
#
#         postgres_metadata = MetaData()
#         postgres_metadata.reflect(bind=postgres_engine)
#         base_table = postgres_metadata.tables.get(baselookup.lower())
#
#         if base_table is None:
#             log.error(f"Table {baselookup} not found in target DB.")
#             return
#
#         for i in range(0, len(processed_results), batch_size):
#             batch = processed_results[i:i + batch_size]
#             data_to_insert = []
#
#             for data in batch:
#                 # Normalize values per data dictionary
#                 for key, value in data.items():
#                     term = db.query(DataDictionaryTerms).filter(
#                         DataDictionaryTerms.dictionary == baselookup,
#                         DataDictionaryTerms.term == key
#                     ).first()
#
#                     if term is None:
#                         continue
#
#                     if value in (None, ''):
#                         data[key] = None
#                     elif term.data_type == "INT":
#                         data[key] = int(value)
#                     elif term.data_type == "BOOLEAN":
#                         data[key] = bool(value)
#                     elif "DATETIME" in term.data_type:
#                         data[key] = convert_datetime_to_iso(value)
#                     elif term.data_type in ("NVARCHAR", "VARCHAR"):
#                         data[key] = str(value)
#                     else:
#                         data[key] = str(value)
#
#                 # Lowercase keys for Postgres
#                 record = {k.lower(): v for k, v in data.items()}
#                 record[id_column] = uuid.uuid4()
#                 data_to_insert.append(record)
#
#             # Insert batch into target DB
#             with postgres_engine.connect() as conn:
#                 conn.execute(insert(base_table).values(data_to_insert))
#                 conn.commit()
#
#             count_inserted += len(data_to_insert)
#             log.info(f"Inserted {len(data_to_insert)} records... total: {count_inserted}")
#
#         log.info(f"✅ Extraction + Load complete. Total: {count_inserted} records.")
#         dqa_check(baselookup, db)
#
#     except Exception as e:
#         error = json.dumps({"status_code": 500, "message": str(e)})
#         log.error("Error loading data ⇒ %s", error)
#         raise Exception("Pipeline failed: " + str(e))

async def extract_data_pipeline(baselookup):
    try:
        # baselookup ="lab"

        db_gen = get_main_db()
        db = next(db_gen)

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

        log.info("===== start loading =====")
        processed_results=[]
        if source_system.conn_type not in ["csv", "api"]:
            await createEngine()

            # extract data from source DB
            with get_source_db() as session:
                log.info("===== this has worked =====")
                log.info(f"===== session =====> {session}")

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
            batch_size = 100
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

                insert_stmt = USLDictionaryModel.insert().values(dataToBeInserted)
                db.execute(insert_stmt)
                db.commit()

                log.info("+++++++ data batch +++++++")
                log.info(f"+++++++ step i : TOTAL LOADED +++++++ {count_inserted} records")

            log.info("+++++++ USL Base Repository Data saved +++++++")

        dqa_check(baselookup, db)

        return {"data": "results processed"}
    except Exception as e:
        error = json.dumps({"status_code":500, "message":e}, default=str)

        log.error("Error loading data ==> %s", str(e))
        raise Exception("Error loading data:: " + str(e))











