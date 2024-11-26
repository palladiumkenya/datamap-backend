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
    credentials = access_credential_list_entity(credentials)
    return {'credentials': credentials}


@router.get('/active_connection')
async def active_connection():
    credential = AccessCredentials.objects().filter(is_active=True).allow_filtering().first()
    return credential


class SaveDBConnection(BaseModel):
    conn_string: str = Field(..., description="Type of the database (e.g., 'mysql', 'postgresql')")
    name: str = Field(..., description="Database username")


@router.post('/add_connection')
async def add_connection(data: SaveDBConnection):
    try:
        AccessCredentials.create(conn_string=data.conn_string, name=data.name)
        return {'success': True, 'message': 'Connection added successfully'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=e)


@router.delete('/delete_connection/{connection_id}')
async def delete_connection(connection_id: str):
    credential = AccessCredentials.objects(id=connection_id).delete()
    return credential


@router.get('/get_connection/{connection_id}')
async def get_connection(connection_id: str):
    credential = AccessCredentials.objects(id=connection_id).first()

    return credential


@router.put('/update_connection/{connection_id}')
async def update_connection(data: SaveDBConnection, connection_id: str):
    AccessCredentials.objects(id=connection_id).update(
        conn_string=data.conn_string,
        name=data.name
    )
    return {"message": "Connection updated successfully", "id": connection_id}


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
    db_url = f"{data.db_type}://{data.username}:{encoded_password}@{data.host_port}/{data.database}"

    success, error_message = test_db(db_url)
    if success:
        return {"status": "Database connection successful"}
    else:
        raise HTTPException(status_code=500, detail=error_message)
