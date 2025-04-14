from fastapi import Depends, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from sqlalchemy import text
from sqlalchemy.orm import Session

from sqlalchemy import desc

from database.database import get_db, execute_data_query
from models.models import AccessCredentials
import json
import uuid
import datetime
from fastapi import APIRouter
from uuid import UUID
import requests
import logging
import settings
from database import database
from settings import settings
from models.models import SiteConfig, TransmissionHistory, DataDictionaries

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

router = APIRouter()


@router.get('/repository/{baselookup}')
async def extracted_data(baselookup: str):
    try:
        query = text(f"""
                   SELECT * FROM {baselookup}
               """)
        results = execute_data_query(query)
        # baseRepoData = [row for row in results]
        return {"data": results}
    except Exception as e:
        print(e)
        log.error("Error fetching extracted data ==> %s", str(e))

        raise HTTPException(status_code=500, detail="An internal error has occurred.")


@router.get('/transmission/history')
async def history(db: Session = Depends(get_db)):
    try:
        dictionaries = db.query(DataDictionaries).all()

        history = []
        for dictionary in dictionaries:
            lastLoaded = db.query(TransmissionHistory).filter(
                    TransmissionHistory.usl_repository_name == dictionary.name, TransmissionHistory.action == 'Loaded')\
                .order_by(desc(TransmissionHistory.facility), desc(TransmissionHistory.created_at)).limit(10).first()
            lastSent = db.query(TransmissionHistory).filter(
                    TransmissionHistory.usl_repository_name == dictionary.name, TransmissionHistory.action == 'Sent')\
                .order_by(desc(TransmissionHistory.facility), desc(TransmissionHistory.created_at)).limit(10).first()

            if lastSent:
                history.append(lastSent)
            if lastLoaded:
                history.append(lastLoaded)

        return {"data": history}
    except Exception as e:
        log.error("Error fetching history data ==> %s", str(e))
        raise HTTPException(status_code=500, detail="An internal error has occurred.")


@router.get('/manifest/repository/{baselookup}')
async def manifest(baselookup: str, db: Session = Depends(get_db)):
    try:
        # cass_session = database.cassandra_session_factory()

        count_query = text(f"""
                           SELECT count(*) count FROM {baselookup}
                       """)
        count = execute_data_query(count_query)
        print(count[0].count)

        columns_query = text(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{baselookup}';
                """)
        columns = execute_data_query(columns_query)

        source_system = db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first()
        site_config = db.query(SiteConfig).filter(SiteConfig.is_active == True).first()

        new_manifest = uuid.uuid1()
        manifest = {
            "manifest_id": new_manifest,
            "usl_repository_name": baselookup.lower(),
            "count": [row.count for row in count][0],
            "columns": [row.column_name for row in columns],
            "session_id": uuid.uuid4(),
            "source_system_name": site_config.primary_system,
            # "source_system_version": source_system['system_version'],
            "source_system_version": "1",
            "opendive_version": "1.0.0",
            "facility_name": site_config.site_name,
            "facility_id": site_config.site_code

        }

        # cass_session.cluster.shutdown()
        log.info(f"+++++++++ NEW MANIFEST ID: {new_manifest} GENERATED +++++++++")

        trans_history = TransmissionHistory(usl_repository_name=baselookup, action="Sent",
                                            facility=f'{site_config.site_name}-{site_config.site_code}',
                                            source_system_id=site_config.id,
                                            source_system_name=site_config.primary_system,
                                            ended_at=None,
                                            manifest_id=new_manifest)
        db.add(trans_history)
        db.commit()
        return manifest
    except Exception as e:
        log.error("Error sending data ==> %s", str(e))

        raise HTTPException(status_code=500, detail=e)
    except BaseException as be:
        log.error("BaseException: Error sending data ==> %s", str(be))

        raise HTTPException(status_code=500, detail=be)


async def send_progress(baselookup: str, manifest: object, websocket: WebSocket, db):
    try:

        totalRecordsquery = text(f"SELECT COUNT(*) as count FROM {baselookup}")
        totalRecordsresult = execute_data_query(totalRecordsquery)

        total_records = totalRecordsresult[0][0]
        # total_records = [row for row in totalRecordsresult][0]["count"]

        # Define batch size (how many records per batch)
        batch_size = settings.BATCH_SIZE
        total_batches = total_records // batch_size + (1 if total_records % batch_size != 0 else 0)

        processed_batches = 0

        select_statement = text(f"""
                                       SELECT * FROM {baselookup} 
                                   """)
        allDataResults = execute_data_query(select_statement)
        allDataResults =  [dict(row._mapping) for row in allDataResults]

        for batch in range(total_batches):
            offset = batch * batch_size
            limit = batch_size
            result = allDataResults[offset:offset + limit]
            log.info("++++++++ off set and limit +++++++", offset, limit)
            baseRepoLoaded = [
                {key: (str(value) if isinstance(value, uuid.UUID)
                       else value.strftime('%Y-%m-%d') if isinstance(value, datetime.date) else value)
                 for key, value in
                 row.items()} for row in result
            ]

            # print('staging to send ', settings.STAGING_API+baselookup)
            log.info('===== USL REPORITORY DATA BATCH LOADED ====== ')

            site_config = db.query(SiteConfig).filter(SiteConfig.is_active == True).first()

            data = {
                "manifest_id": manifest["manifest_id"],
                "batch_no": batch,
                "total_batches": total_batches,
                "facility": site_config.site_name,
                "facility_id": site_config.site_code,
                "data": baseRepoLoaded
            }

            log.info(f'===== STARTED SENDING DATA TO STAGING_API ===== Batch No. {batch}')
            res = requests.post(settings.STAGING_API + baselookup,
                                json=data)
            log.info(f'===== SUCCESSFULLY SENT BATCH No. {batch} TO STAGING_API ===== Status Code :{res.status_code} ')

            # Increment processed batches
            processed_batches += 1
            progress = int((processed_batches / total_batches) * 100)

            # Send the progress to the WebSocket
            await websocket.send_text(f"{progress}")

        # websocket.send_text(f"100")
        await websocket.close()
        log.info("++++++++++ All batches loaded and sent +++++++++++")
        return {"status_code": 200, "message": "suuccessfully sent batches"}
    except Exception as e:
        log.error("Error sending data ==> %s", str(e))
        await websocket.send_text(f"error ocurred")
        # await websocket.close()
        raise HTTPException(status_code=500, detail=e)
    except BaseException as be:
        log.error("BaseException: Error sending data ==> %s", str(be))
        await websocket.send_text(f"error ocurred")
        # await websocket.close()
        raise HTTPException(status_code=500, detail=be)


@router.websocket("/ws/progress/{baselookup}")
async def websocket_endpoint(baselookup: str, websocket: WebSocket, db: Session = Depends(get_db)):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            manifest = json.loads(data)
            print("websocket manifest -->", manifest)
            await send_progress(baselookup, manifest, websocket, db)
    except WebSocketDisconnect:
        log.error("Client disconnected")
    except Exception as e:
        log.error("Websocket error ==> %s", str(e))
        # await websocket.close()

# @router.websocket("/ws/progress")
# async def progress_updates(websocket: WebSocket):
#    await websocket.accept()
#    try:
#        for i in range(101):  # Simulating progress (0-100%)
#            await websocket.send_text(f"{i}")
#            await asyncio.sleep(0.1)  # Simulate work delay
#    except WebSocketDisconnect:
#        print("Client disconnected")
