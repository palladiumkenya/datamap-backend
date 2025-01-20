import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from settings import settings
from sqlalchemy.ext.declarative import declarative_base




log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)



DATABASE_URL = (
    f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@"
    f"{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
)

# Create the SQLAlchemy engine
usl_db_engine = create_engine(DATABASE_URL)

# Create a SessionLocal class for database sessions
UslDBSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=usl_db_engine)
UslDbBase = declarative_base()

# Dependency to get a database session
def get_usldb_database():
    """
    Dependency that provides a database session.
    Closes the session after the request is completed.
    """
    db = UslDBSessionLocal()  # Create a new session
    try:
        yield db
    finally:
        db.close()  # Ensure the session is closed


