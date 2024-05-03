from fastapi import APIRouter, HTTPException, Depends
from models.models import User
from auth import create_access_token, hash_password, verify_password
from models.models import AccessCredentials
from database import database
from serializers.access_credentials_serializer import access_credential_list_entity
from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm

router = APIRouter()


class UserBase(BaseModel):
    username: str
    password: str


@router.post('/register')
async def register_user(user: UserBase):
    if User.objects(username=user.username):
        raise HTTPException(
            status_code=400, detail="Username already registered")
    hashed_password = hash_password(user.password)
    User.create(username=user.username, password=hashed_password)
    return {"message": "User created successfully"}


@router.get('/{username}')
async def get_user(username: str):
    user = User.objects(username=username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"username": user.username, "created_at": user.created_at}


@router.post('/login')
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    user = User.objects(username=form_data.username).first()
    if not user or not verify_password(form_data.password, user.password):
        raise HTTPException(
            status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}
