from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError, ExpiredSignatureError
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from database.user_db import get_user_db
from models.user_model import User
from settings import settings

SECRET_KEY = settings.JWT_SECRET_KEY
REFRESH_SECRET_KEY = settings.REFRESH_SECRET_KEY
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 60  # One hour
REFRESH_TOKEN_EXPIRE_DAYS = 7

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/user/login")


pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_token(data:dict, secret_key: str, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode["exp"] = expire
    return jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)


def create_access_token(data: dict):
    return create_token(data, SECRET_KEY, timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))


def create_refresh_token(data: dict):
    return create_token(data, REFRESH_SECRET_KEY, timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS))


def verify_token(token: str, secret_key: str):
    try:
        return jwt.decode(token, secret_key, algorithms=[ALGORITHM])
    except ExpiredSignatureError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired") from e
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e)
        ) from e


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_user_db)):
    payload = verify_token(token, SECRET_KEY)
    email = payload.get("sub")
    if email is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token is invalid")
    db_user = db.query(User).filter(User.email == email).first()
    if db_user is None:
        raise HTTPException(status_code=status.HTTP_40_BAD_REQUEST, detail="User not found")
    return db_user


def seed_default_user(db: Session):
    """
        Seed the database with a default user if none exists.
        This function checks the database for an existing user. If no user is found, it creates a default user with specified credentials and saves it to the database.
        Args:
            db (Session): The database session used to interact with the database.
        Returns:
            None
        """
    existing_user = db.query(User).first()
    if not existing_user:
        default_user = User(
            email="admin@local.test",
            username="admin",
            password=hash_password("Admin123"),
            is_active=True
        )
        db.add(default_user)
        db.commit()
        db.refresh(default_user)
