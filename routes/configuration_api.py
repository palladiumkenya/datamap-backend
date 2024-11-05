import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from models.models import UniversalDictionaryConfig
from serializers.universal_dictionary_config_serializer import universal_dictionary_config_serializer_entity

router = APIRouter()


@router.get("/get_dictionary_config")
def get_dictionary_config():
    try:
        configs = UniversalDictionaryConfig.objects.first()
        if configs is not None:
            response_config = universal_dictionary_config_serializer_entity(configs)
        else:
            response_config = None
        return {"data": response_config}
    except UniversalDictionaryConfig.DoesNotExist:
        raise HTTPException(status_code=404, detail="Config not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class SaveUniversalDataDictionary(BaseModel):
    universal_dictionary_url: str = Field(..., description="")
    universal_dictionary_jwt: str = Field(..., description="")
    universal_dictionary_update_frequency: str = Field(..., description="")


@router.post("/add_dictionary_config")
def add_dictionary_config(data: SaveUniversalDataDictionary):
    try:
        dictionary_config = UniversalDictionaryConfig.objects.first()
        if dictionary_config is not None:
            dictionary_config.universal_dictionary_url = data.universal_dictionary_url
            dictionary_config.universal_dictionary_jwt = data.universal_dictionary_jwt
            dictionary_config.universal_dictionary_update_frequency = data.universal_dictionary_update_frequency
            dictionary_config.save()
        else:
            dictionary_config = UniversalDictionaryConfig(
                universal_dictionary_url=data.universal_dictionary_url,
                universal_dictionary_jwt=data.universal_dictionary_jwt,
                universal_dictionary_update_frequency=data.universal_dictionary_update_frequency
            )
            dictionary_config.save()
        return {"status": "success", "data": dictionary_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class TestUniversalDataDictionary(BaseModel):
    universal_dictionary_url: str = Field(..., description="")
    universal_dictionary_jwt: str = Field(..., description="")


@router.post("/test_dictionary_config")
def test_dictionary_config(
        data: TestUniversalDataDictionary
):
    headers = {"Authorization": f"Bearer {data.universal_dictionary_jwt}"}
    try:
        response = requests.get(data.universal_dictionary_url, headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.json())
        return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class UpdateUniversalDataDictionary(BaseModel):
    universal_dictionary_url: str = Field(..., description="")
    universal_dictionary_jwt: str = Field(..., description="")
    universal_dictionary_update_frequency: str = Field(..., description="")


# @router.put("/update_dictionary_config")
# def update_dictionary_config(data: UpdateUniversalDataDictionary):
#     try:
#         update = UniversalDictionaryConfig.objects.first()
#         update.universal_dictionary_url = data.universal_dictionary_url
#         update.universal_dictionary_jwt = data.universal_dictionary_jwt
#         update.universal_dictionary_update_frequency = data.universal_dictionary_update_frequency
#         update.save()
#
#         return {"status": "success"}
#     except UniversalDictionaryConfig.DoesNotExist:
#         raise HTTPException(status_code=404, detail="Config not found")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
