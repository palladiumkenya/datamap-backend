from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import (access_api, dictionary_mapper_api, data_dictionary_api, data_dictionary_usl_api, configuration_api,
                    usl_data_transmission_api, site_configuration_api, user_management, transformations_api,
                    flatfile_mapper_api, mappings_configs_api, data_extraction_api, appsettings_configuration_api)
from models import models
from models import usl_models
from database.user_db import UserBase, user_engine, SessionLocal
from database.database import engine, SQL_DATABASE_URL
from routes.access_api import test_db

from utils.user_utils import seed_default_user



app = FastAPI()

success, error_message = test_db(SQL_DATABASE_URL)
if success:
    models.Base.metadata.create_all(engine)
    usl_models.Base.metadata.create_all(engine)

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    db = SessionLocal()
    try:
        seed_default_user(db)
    finally:
        db.close()


UserBase.metadata.create_all(bind=user_engine)

app.include_router(access_api.router, tags=['Access'], prefix='/api/db_access')
app.include_router(dictionary_mapper_api.router, tags=['Mapper'], prefix='/api/dictionary_mapper')
app.include_router(flatfile_mapper_api.router, tags=['FlatFileMapper'], prefix='/api/flatfile_mapper')
app.include_router(mappings_configs_api.router, tags=['MapperConfigs'], prefix='/api/mappings_config')
app.include_router(usl_data_transmission_api.router, tags=['Transmission'], prefix='/api/usl_data')
app.include_router(data_extraction_api.router, tags=['Extraction'], prefix='/api/extract')
app.include_router(data_dictionary_api.router, tags=['Data Dictionary'], prefix='/api/data_dictionary')
app.include_router(configuration_api.router, tags=['App Configuration'], prefix='/api/config')
app.include_router(site_configuration_api.router, tags=['Site Configuration'], prefix='/api/site_config')
app.include_router(transformations_api.router, tags=['DQA Configuration'], prefix='/api/dqa')
app.include_router(data_dictionary_usl_api.router, tags=['USL Data Dictionary'], prefix='/api/usl/data_dictionary')
app.include_router(appsettings_configuration_api.router, tags=['App Settings'], prefix='/api/appsettings')

# TODO: MOVE READ TO ELSEWHERE
# app.include_router(text2sql_api.router, tags=['Text2SQL'], prefix='/api/text2sql')


@app.get("/api/healthchecker")
def root():
    return {"message": "Welcome to data map, we are up and running"}


# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
