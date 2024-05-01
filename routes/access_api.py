from fastapi import APIRouter

from models.models import AccessCredentials
from database import database
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
