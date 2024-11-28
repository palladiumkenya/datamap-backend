from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database.user_db import get_db
from models.user_model import User
from schemas.user_schema import UserResponse, UserCreate
from utils.user_utils import hash_password, verify_password, create_access_token

router = APIRouter()


@router.post("/register", response_model=UserResponse)
async def register(user: UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    new_user = User(
        username=user.username,
        email=user.email,
        password=hash_password(user.password)
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


@router.post("/login")
async def user_login(email: str, password: str, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.email == email).first()
    if not db_user or not verify_password(password, db_user.password):
        raise HTTPException(status_code=400, detail="Incorrect email or password")

    token = create_access_token(data={"sub": db_user.email})

    return {"token": token, "token_type": "bearer"}
