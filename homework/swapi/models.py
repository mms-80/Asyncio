import os

from sqlalchemy import String, Text
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret")
POSTGRES_USER = os.getenv("POSTGRES_USER", "swapi")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5431")

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class SwapiPeople(Base):
    __tablename__ = "swapi_people"

    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column(String(100))
    eye_color: Mapped[str] = mapped_column(String(50))
    films: Mapped[str] = mapped_column(Text)
    gender: Mapped[str] = mapped_column(String(50))
    hair_color: Mapped[str] = mapped_column(String(50))
    height: Mapped[str] = mapped_column(String(50))
    homeworld: Mapped[str] = mapped_column(String(50))
    mass: Mapped[str] = mapped_column(String(10))
    name: Mapped[str] = mapped_column(String(50))
    skin_color: Mapped[str] = mapped_column(String(50))
    species: Mapped[str] = mapped_column(Text)
    starships: Mapped[str] = mapped_column(Text)
    vehicles: Mapped[str] = mapped_column(Text)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
