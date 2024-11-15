from fastapi import  Depends, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
import asyncio
import time
from sqlalchemy.orm import sessionmaker, Session
import uuid
import datetime
from fastapi import APIRouter
import requests
import logging
import settings
from models.models import AccessCredentials,MappedVariables, DataDictionaryTerms, DataDictionaries
from database import database
from serializers.dictionary_mapper_serializer import indicator_selector_entity,indicator_selector_list_entity
from serializers.access_credentials_serializer import access_credential_list_entity
from settings import settings




log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)




router = APIRouter()





@router.get('/repository/{baselookup}')
async def extracted_data(baselookup:str):
    try:
        cass_session = database.cassandra_session_factory()

        query = f"""
                   SELECT * FROM {baselookup}
               """
        baseRepoData = cass_session.execute(query)

        cass_session.cluster.shutdown()
        return baseRepoData
    except Exception as e:
        log.error("Error fetching extracted data ==> %s", str(e))

        return e




@router.get('/send/usl/{baselookup}')
async def send_data(baselookup:str, background_tasks: BackgroundTasks):
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


@router.get('/manifest/repository/{baselookup}')
async def manifest(baselookup:str):
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




@router.websocket("/ws/progress")
async def progress_updates(websocket: WebSocket):
   await websocket.accept()
   try:
       for i in range(101):  # Simulating progress (0-100%)
           await websocket.send_text(f"{i}")
           await asyncio.sleep(0.1)  # Simulate work delay
   except WebSocketDisconnect:
       print("Client disconnected")

# # Dictionary to store WebSocket connections and their associated progress
# connections = {}
# @router.websocket("/ws/progress")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     # Store the connection to send progress updates later
#     connections[websocket] = 0
#     print("websocket connections ==>", connections)
#
#     try:
#         while True:
#             # Keep the connection open to send updates
#             await websocket.receive_text()
#             print("websocket ==>", websocket)
#     except:
#         # Clean up the WebSocket connection on error or disconnect
#         del connections[websocket]
#         await websocket.close()
#         print("clean up websocket ==>", websocket)

