from sqlalchemy import create_engine, String, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from settings import settings

# Cassandra Connection
print('Connecting to Cassandra...')
engine = create_engine(f"cassandra://{settings.CASSANDRA_USER}:{settings.CASSANDRA_PASSWORD}@{settings.CASSANDRA_HOST}:{settings.CASSANDRA_PORT}/?Database={settings.CASSANDRA_DATABASE}")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
print('Connected to Cassandra...')

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
