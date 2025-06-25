import requests
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from settings import settings
from routes.access_api import test_db
import uuid
from database.database import get_db
from models.models import SiteConfig

router = APIRouter()




class EnvUpdate(BaseModel):
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str

@router.put("/update")
def update_env(data: EnvUpdate):
    try:
        env_vars = data.dict()
        with open(".env", "r") as file:
            lines = file.readlines()

        updated_keys = set()
        new_lines = []

        for line in lines:
            key = line.split("=")[0]
            if key in env_vars:
                new_lines.append(f"{key}={env_vars[key]}\n")
                updated_keys.add(key)
            else:
                new_lines.append(line)

        for key, value in env_vars.items():
            if key not in updated_keys:
                new_lines.append(f"{key}={value}\n")

        with open(".env", "w") as file:
            file.writelines(new_lines)

        return {"status": "success", "data": "Successfully updated appsettings"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/configs")
def get_site_configs():
    appsettings = {"DB_USER": settings.DB_USER, "DB_PASSWORD": settings.DB_PASSWORD, "DB_HOST": settings.DB_HOST,
                   "DB_PORT": settings.DB_PORT, "DB_NAME": settings.DB_NAME}
    return {"data": appsettings}


@router.post("/test")
def test_env(data: EnvUpdate):
    env_vars = data.dict()

    app_db_url = f'postgresql://{env_vars["DB_USER"]}:{env_vars["DB_PASSWORD"]}@{env_vars["DB_HOST"]}:{env_vars["DB_PORT"]}/{env_vars["DB_NAME"]}'

    success, error_message = test_db(app_db_url)
    if success:
        return {"status": "Database connection successful"}
    else:
        raise HTTPException(status_code=500, detail=error_message)
