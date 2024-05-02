from urllib.parse import quote_plus
from fastapi import APIRouter, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from database import database
from models.models import AccessCredentials
from serializers.access_credentials_serializer import access_credential_list_entity

router = APIRouter()


@router.get('/available_connections')
async def available_connections():
    credentials = AccessCredentials.objects().all()
    # print(credentials)
    credentials = access_credential_list_entity(credentials)
    return {'credentials': credentials}


@router.post('/add_connection')
async def add_connection(conn_string: str):
    credential = AccessCredentials.create(conn_string=conn_string)
    # credentials = access_credential_list_entity(credential)
    return credential


@router.delete('/delete_connection')
async def delete_connection(_id: str):
    credential = AccessCredentials.objects(id=_id).delete()
    return credential


@router.put('/update_connection')
async def update_connection(_id: str, conn_string: str):
    credential = AccessCredentials.objects(id=_id).update(conn_string=conn_string)
    # credential = credential
    return credential


def test_db(db_url):
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
        conn.close()
        return True, None
    except OperationalError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


@router.get("/test_db_connection")
async def test_db_connection(db_type: str, host: str, port: int, database: str, username: str, password: str):
    # Encode special characters in the password
    encoded_password = quote_plus(password)
    db_url = f"{db_type}://{username}:{encoded_password}@{host}:{port}/{database}"

    success, error_message = test_db(db_url)
    if success:
        return {"status": "Database connection successful"}
    else:
        raise HTTPException(status_code=500, detail=error_message)
