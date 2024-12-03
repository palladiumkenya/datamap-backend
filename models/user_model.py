from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from database.user_db import UserBase

metadata = UserBase.metadata


class User(UserBase):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    password = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))
