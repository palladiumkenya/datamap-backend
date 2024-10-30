from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from models.models import UniversalDictionaryConfig
from serializers.universal_dictionary_config_serializer import universal_dictionary_config_serializer_entity

router = APIRouter()


@router.get("/get_dictionary_config")
def get_dictionary_config():
    try:
        configs = UniversalDictionaryConfig.objects.first()
        response_config = universal_dictionary_config_serializer_entity(configs)
        return response_config
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
        data_dictionary = UniversalDictionaryConfig(
            universal_dictionary_url=data.universal_dictionary_url,
            universal_dictionary_jwt=data.universal_dictionary_jwt,
            universal_dictionary_update_frequency=data.universal_dictionary_update_frequency
        )
        data_dictionary.save()
        return {"status": "success", "data": data_dictionary}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class TestUniversalDataDictionary(BaseModel):
    universal_dictionary_url: str = Field(..., description="")
    universal_dictionary_jwt: str = Field(..., description="")


@router.post("/test_dictionary_config")
def test_dictionary_config(
        data: TestUniversalDataDictionary
):

    return


class UpdateUniversalDataDictionary(BaseModel):
    universal_dictionary_url: str = Field(..., description="")
    universal_dictionary_jwt: str = Field(..., description="")
    universal_dictionary_update_frequency: str = Field(..., description="")


@router.put("/update_dictionary_config")
def update_dictionary_config(data: UpdateUniversalDataDictionary):
    try:
        update = UpdateUniversalDataDictionary.objects.first()
        update.universal_dictionary_url = data.universal_dictionary_url
        update.universal_dictionary_jwt = data.universal_dictionary_jwt
        update.universal_dictionary_update_frequency = data.universal_dictionary_update_frequency
        update.save()

        return {"status": "success"}
    except UpdateUniversalDataDictionary.DoesNotExist:
        raise HTTPException(status_code=404, detail="Config not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
