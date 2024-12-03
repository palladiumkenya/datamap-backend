import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from models.models import SiteConfig
from serializers.site_config_serializer import site_config_serializer_entity, site_config_list_entity

router = APIRouter()


@router.get("/all/configs")
def get_site_configs():
    try:
        configs = SiteConfig.objects.all()
        if configs is not None:
            response_config = [config for config in configs]
        else:
            response_config = None
        return {"data": response_config}
    except SiteConfig.DoesNotExist:
        raise HTTPException(status_code=404, detail="Config not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



@router.get("/active_site_config")
def get_active_site_config():
    try:
        config = SiteConfig.objects.filter(is_active=True).allow_filtering().first()
        if config is not None:
            response_config = config
        else:
            response_config = None
        return {"data": response_config}
    except SiteConfig.DoesNotExist:
        raise HTTPException(status_code=404, detail="Config not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class SaveSiteConfig(BaseModel):
    site_name: str = Field(..., description="")
    site_code: str = Field(..., description="")
    primary_system: str = Field(..., description="")
    is_active: bool = Field(..., description="")


@router.post("/add/config")
def add_site_config(data: SaveSiteConfig):
    try:
        if data.is_active:
            site_configs = SiteConfig.objects.all()
            if site_configs is not None:
                for config in site_configs:
                    config.is_active = False
                    config.save()

        site_config = SiteConfig(
            site_name=data.site_name,
            site_code=data.site_code,
            primary_system=data.primary_system,
            is_active=data.is_active
        )
        site_config.save()

        return {"status": "success", "data": site_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



@router.put("/edit/config")
def edit_site_config(data: SaveSiteConfig):
    try:
        if data.is_active:
            site_configs = SiteConfig.objects.all()
            if site_configs is not None:
                for config in site_configs:
                    config.is_active = False
                    config.save()

        site_config = SiteConfig.objects.first()
        if site_config is not None:
            site_config.site_name = data.site_name
            site_config.site_code = data.site_code
            site_config.primary_system = data.primary_system
            site_config.is_active = data.is_active

            site_config.save()

        return {"status": "success", "data": site_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/delete/config/{config_id}")
def delete_site_config(config_id: str):
    try:
        site_config = SiteConfig.objects(id=config_id).delete()

        return {"status": "success", "data": site_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



