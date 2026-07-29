import os
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Integer, DateTime, Text

DATABASE_URL = "sqlite+aiosqlite:///./data/sessions.db"
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

def get_utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(String, primary_key=True, index=True)
    container_id = Column(String, nullable=False)
    vnc_port = Column(Integer, nullable=False)
    status = Column(String, default="active")
    created_at = Column(DateTime, default=get_utc_now)

class MessageModel(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String, index=True)
    role = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=get_utc_now)

async def init_db():
    os.makedirs("./data", exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)