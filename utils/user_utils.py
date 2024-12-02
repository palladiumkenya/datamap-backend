from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, status, Depends
from jose import jwt, JWTError
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from database.user_db import get_db
from models.user_model import User
from settings import settings

SECRET_KEY = settings.JWT_SECRET_KEY
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    to_encode['exp'] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def verify_token(token: str):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e)
        ) from e


def get_current_user(token: str = Depends(verify_token), db: Session = Depends(get_db)):
    email = token.sub("sub")
    if email is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Token is invalid")
    db_user = db.query(User).filter(User.email == email).first()
    if db_user is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User not found")
    return db_user
