import requests
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
import uuid
from database.database import get_db
from models.models import SiteConfig

router = APIRouter()


@router.get("/all/configs")
def get_site_configs(db: Session = Depends(get_db)):
    try:
        configs = db.query(SiteConfig).all()
        return {"data": configs}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/active_site_config")
def get_active_site_config(db: Session = Depends(get_db)):
    try:
        config = db.query(SiteConfig).filter(SiteConfig.is_active == True).first()
        return {"data": config}
    except NoResultFound:
        raise HTTPException(status_code=404, detail="Config not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/details/{config_id}")
def get_site_config(config_id: str, db: Session = Depends(get_db)):
    try:
        config = db.query(SiteConfig).filter(SiteConfig.id == config_id).first()
        return {"data": config}
    except NoResultFound:
        raise HTTPException(status_code=404, detail="Config not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class SaveSiteConfig(BaseModel):
    site_name: str = Field(..., description="")
    site_code: str = Field(..., description="")
    primary_system: str = Field(..., description="")
    country: str = Field(..., description="")
    region: str = Field(..., description="")
    organization: str = Field(..., description="")

    is_active: bool = Field(..., description="")


@router.post("/add/config")
def add_site_config(data: SaveSiteConfig, db: Session = Depends(get_db)):
    try:
        if data.is_active:
            db.query(SiteConfig).filter(
                SiteConfig.is_active == True
            ).update({
                'is_active': False
            })
            db.commit()

        site_config = SiteConfig(
            site_name=data.site_name,
            site_code=data.site_code,
            primary_system=data.primary_system,
            country=data.country,
            region=data.region,
            organization=data.organization,
            is_active=data.is_active
        )
        db.add(site_config)
        db.commit()

        return {"status": "success", "data": site_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


# @router.put("/edit/config")
@router.put("/edit/config/{config_id}")
def edit_site_config(config_id: str, data: SaveSiteConfig, db: Session = Depends(get_db)):
    try:
        # deactivate other active site if updated site is set as default
        if data.is_active:
            active_config = db.query(SiteConfig).filter(SiteConfig.is_active == True, SiteConfig.id != config_id).first()

            if active_config is not None:
                active_config.is_active = False


        # update site config
        db.query(SiteConfig).filter(SiteConfig.id == config_id).update({
            "site_name": data.site_name,
            "site_code": data.site_code,
            "primary_system": data.primary_system,
            "country": data.country,
            "region": data.region,
            "organization": data.organization,
            "is_active": data.is_active
        })
        db.commit()
        return {"status": "success", "data": config_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/delete/config/{config_id}")
def delete_site_config(config_id: str, db: Session = Depends(get_db)):
    try:
        site_config = db.query(SiteConfig).filter(SiteConfig.id == config_id).delete()
        db.commit()

        return {"status": "success", "data": site_config}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
