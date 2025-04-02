from urllib.parse import quote_plus

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from database.database import get_db
from models.models import AccessCredentials, SiteConfig
from serializers.access_credentials_serializer import access_credential_list_entity, systems_list_entity, system_entity, \
    access_credential_entity
from utils.data_upload_handler import upload_data

router = APIRouter()


@router.get('/available_connections')
async def available_connections(db: Session = Depends(get_db)):
    credentials = db.query(AccessCredentials).all()
    credentials = access_credential_list_entity(credentials)
    return {'credentials': credentials}


@router.get('/active_connection')
async def active_connection(db: Session = Depends(get_db)):
    active_credentials = (
        db
        .query(AccessCredentials)
        .filter(AccessCredentials.is_active==True)
        .first()
    )

    if active_credentials is None:
        return {"error": "No active credentials found"}

    credentials = access_credential_entity(active_credentials)
    if credentials is not None:
        credentials['site'] = system_entity(active_credentials.system)
        return credentials

    return {"error": "Failed to process active credentials"}


class SaveDBConnection(BaseModel):
    conn_string: str = Field(..., description="Type of the database (e.g., 'mysql', 'postgresql')")
    name: str = Field(..., description="Connection name")
    conn_type: str = Field(..., description="Connection Type")
    system_id: str = Field(..., description="System ID")


@router.post('/add_connection')
async def add_connection(data: SaveDBConnection, db: Session = Depends(get_db)):
    try:
        db.query(AccessCredentials).filter(AccessCredentials.is_active==True).update({
            AccessCredentials.is_active: False
        })
        db.commit()
        credential = AccessCredentials(
            conn_string=data.conn_string, name=data.name, system_id=data.system_id, conn_type=data.conn_type
        )
        db.add(credential)
        db.commit()
        return {'success': True, 'message': 'Connection added successfully'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=e) from e


class SaveUploadData(BaseModel):
    name: str = Field(..., description="Name of connection")
    data: list = Field(..., description="")
    upload: str = Field(..., description="the upload source", examples=["csv", "api"])


@router.post('/upload_data')
async def upload_data_handler(data: SaveUploadData, background_tasks: BackgroundTasks = BackgroundTasks()):
    background_tasks.add_task(upload_data, data)
    return {'message': 'Upload started'}


@router.delete('/delete_connection/{connection_id}')
async def delete_connection(connection_id: str, db: Session = Depends(get_db)):
    try:
        db.query(AccessCredentials).filter(AccessCredentials.id == connection_id).delete()
        db.commit()
        return {'success': True, 'message': 'Connection Deleted'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=e) from e


@router.get('/get_connection/{connection_id}')
async def get_connection(connection_id: str, db: Session = Depends(get_db)):
    return db.query(AccessCredentials).filter(AccessCredentials.id == connection_id).first()


@router.put('/update_connection/{connection_id}')
async def update_connection(data: SaveDBConnection, connection_id: str, db: Session = Depends(get_db)):
    db.query(AccessCredentials).filter(AccessCredentials.id == connection_id).update({
        AccessCredentials.conn_string: data.conn_string,
        AccessCredentials.name: data.name,
        AccessCredentials.conn_type: data.conn_type,
        AccessCredentials.system_id: data.system_id
    })
    db.commit()
    return {"message": "Connection updated successfully", "id": connection_id}


def test_db(db_url):
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
        conn.close()
        return True, None
    except (OperationalError, Exception) as e:
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


@router.get("/dictionary/systems")
def get_system_list(db: Session = Depends(get_db)):
    systems = db.query(SiteConfig).filter(SiteConfig.is_active == True).all()
    systems_list = systems_list_entity(systems)
    return {
        "data": systems_list,
        "success": True
    }
