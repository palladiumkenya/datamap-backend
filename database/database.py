import logging

from settings import settings

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib.parse

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


SQL_DATABASE_URL = f'postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB}'

engine = create_engine(
    SQL_DATABASE_URL, connect_args={}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def execute_data_query(query):
    with engine.connect() as connection:
        result = connection.execute(query)
        return result.fetchall()


def execute_query(query, values=None):
    with engine.connect() as connection:
        if values:
            result = connection.execute(query, values)
        else:
            result = connection.execute(query)
        connection.commit()
        return result.rowcount
