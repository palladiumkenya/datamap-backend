import logging
from fastapi import Depends
from settings import settings
from contextlib import contextmanager
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine, inspect, MetaData, Table, text
from sqlalchemy.ext.declarative import declarative_base
import urllib.parse
from serializers.access_credentials_serializer import access_credential_entity
from database.database import execute_data_query, get_db as get_main_db, execute_query, engine as postgres_engine
from models.models import AccessCredentials
from routes.access_api import test_db




log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


# # Create an inspector object to inspect the database
engine = None
inspector = None
metadata = None
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def createSourceDbEngine():
    global engine, inspector, metadata
    db_gen = get_main_db()
    db = next(db_gen)

    try:
        active_credentials = (db.query(AccessCredentials).filter(AccessCredentials.is_active == True).first())
        credentials = access_credential_entity(active_credentials)
        if credentials and credentials["conn_type"] not in ["csv", "api"]:
            log.info('===== start creating an engine =====')
            connection_string = credentials["conn_string"]
            success, error_message = test_db(connection_string)
            if success:
                engine = create_engine(connection_string)
                inspector = inspect(engine)
                metadata = MetaData()
                metadata.reflect(bind=engine)
                log.info('===== Database reflected ====')
                return engine
            else:
                log.error("Cannot connect to DB using connection string provided")

    except SQLAlchemyError as e:
        # Log the error or handle it as needed
        log.error('===== Database not reflected ==== ERROR:', str(e))
        raise HTTPException(status_code=500, detail="Database connection error" + str(e)) from e


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# SessionLocal.configure(bind=engine)

Base = declarative_base()

@contextmanager
def get_source_db():
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()




# SQL_DATABASE_URL = f'postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB}'
#
# engine = create_engine(
#     SQL_DATABASE_URL, connect_args={}
# )
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)




