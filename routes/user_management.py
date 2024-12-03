from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database.user_db import get_user_db
from models.user_model import User
from schemas.user_schema import UserResponse, UserCreate, TokenRefresh, UserLogin
from utils.user_utils import hash_password, verify_password, create_access_token, get_current_user, \
    create_refresh_token, verify_token, REFRESH_SECRET_KEY

router = APIRouter()


@router.post("/register", response_model=UserResponse)
async def register(user: UserCreate, db: Session = Depends(get_user_db)):
    db_email = db.query(User).filter(User.email == user.email).first()
    db_username = db.query(User).filter(User.username == user.username).first()
    if db_email:
        raise HTTPException(status_code=400, detail="Email already registered")
    if db_username:
        raise HTTPException(status_code=400, detail="Username already registered")

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
async def user_login(user: UserLogin, db: Session = Depends(get_user_db)):
    db_user = db.query(User).filter(User.email == user.email).first()
    if not db_user or not verify_password(user.password, db_user.password):
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    elif not db_user.is_active:
        raise HTTPException(status_code=400, detail="User is not active")

    access_token = create_access_token(data={"sub": db_user.email})
    refresh_token = create_refresh_token(data={"sub": db_user.email})

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }


@router.post("/refresh")
def refresh_access_token(refresh_token: TokenRefresh, db: Session = Depends(get_user_db)):
    # Verify the refresh token
    payload = verify_token(refresh_token.refresh_token, REFRESH_SECRET_KEY)
    email = payload.get("sub")
    if email is None:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    # Check if the user exists
    db_user = db.query(User).filter(User.email == email).first()
    if db_user is None:
        raise HTTPException(status_code=401, detail="User not found")

    # Generate a new access token
    new_access_token = create_access_token(data={"sub": db_user.email})
    return {
        "access_token": new_access_token,
        "token_type": "bearer"
    }


@router.get('/info', response_model=UserResponse)
async def user_info(current_user: User = Depends(get_current_user)):
    return current_user
