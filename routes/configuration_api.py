import requests
from fastapi import APIRouter, HTTPException, Depends
from urllib.parse import urlparse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database.database import get_db
from models.models import UniversalDictionaryConfig
from serializers.universal_dictionary_config_serializer import universal_dictionary_config_serializer_entity

router = APIRouter()


@router.get("/get_dictionary_config")
def get_dictionary_config(db: Session = Depends(get_db)):
    try:
        configs = db.query(UniversalDictionaryConfig).first()
        if configs is not None:
            response_config = universal_dictionary_config_serializer_entity(configs)
        else:
            raise HTTPException(status_code=404, detail="Config not found")
        return {"data": response_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class SaveUniversalDataDictionary(BaseModel):
    universal_dictionary_url: str = Field(..., description="")
    universal_dictionary_jwt: str = Field(..., description="")
    universal_dictionary_update_frequency: str = Field(..., description="")


@router.post("/add_dictionary_config")
def add_dictionary_config(data: SaveUniversalDataDictionary, db: Session = Depends(get_db)):
    try:
        dictionary_config = db.query(UniversalDictionaryConfig).first()
        if dictionary_config is not None:
            dictionary_config.universal_dictionary_url = data.universal_dictionary_url
            dictionary_config.universal_dictionary_jwt = data.universal_dictionary_jwt
            dictionary_config.universal_dictionary_update_frequency = data.universal_dictionary_update_frequency
        else:
            dictionary_config = UniversalDictionaryConfig(
                universal_dictionary_url=data.universal_dictionary_url,
                universal_dictionary_jwt=data.universal_dictionary_jwt,
                universal_dictionary_update_frequency=data.universal_dictionary_update_frequency
            )
        db.add(dictionary_config)
        db.commit()
        return {"status": "success", "data": dictionary_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class TestUniversalDataDictionary(BaseModel):
    universal_dictionary_url: str = Field(..., description="")
    universal_dictionary_jwt: str = Field(..., description="")


def is_valid_url(url):
    parsed_url = urlparse(url)
    return parsed_url.scheme in ("http", "https")


@router.post("/test_dictionary_config")
def dictionary_config_test(
        data: TestUniversalDataDictionary
):
    headers = {"Authorization": f"Bearer {data.universal_dictionary_jwt}"}
    try:
        if not is_valid_url(data.universal_dictionary_url):
            raise HTTPException(status_code=404, detail="Invalid URL")
        response = requests.get(data.universal_dictionary_url, headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
        return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
