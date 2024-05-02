from urllib.parse import quote_plus
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
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


class DBConnectionRequest(BaseModel):
    db_type: str = Field(..., description="Type of the database (e.g., 'mysql', 'postgresql')")
    host_port: str = Field(..., description="Database host & port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")


@router.post("/test_db_connection")
async def test_db_connection(data: DBConnectionRequest):
    # Encode special characters in the password
    encoded_password = quote_plus(data.password)
    db_url = f"{data.db_type}://{data.username}:{encoded_password}@{data.host_port}/{database}"

    success, error_message = test_db(db_url)
    if success:
        return {"status": "Database connection successful"}
    else:
        raise HTTPException(status_code=500, detail=error_message)
