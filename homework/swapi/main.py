import asyncio
from datetime import datetime
from typing import AsyncGenerator, Union

import requests
from aiohttp import ClientSession
from models import Session, SwapiPeople, init_db
from more_itertools import chunked

MAX_CHUNK = 5
PEOPLE_CNT = requests.get("https://swapi.py4e.com/api/people/").json()["count"]


async def get_person(person_id: str, session: ClientSession) -> dict:
    """Получение информации о конкретном персонаже"""
    http_response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    if http_response.status == 404:
        return {"status": 404}
    json_data = await http_response.json()
    return json_data


async def get_url(url: str, key: str, session: ClientSession) -> str:
    """Функция для отправки запроса по конкретному url"""
    try:
        async with session.get(f"{url}") as response:
            data = await response.json()
            return data[key]
    except Exception:
        return "error"


async def tasks_urls(
    urls: Union[list | str], key: str, session: ClientSession
) -> AsyncGenerator:
    """Функция создания задачи по отправки запроса"""
    if isinstance(urls, list):
        tasks = (asyncio.create_task(get_url(url, key, session)) for url in urls)
        for task in tasks:
            yield await task
    elif isinstance(urls, str):
        yield await asyncio.create_task(get_url(urls, key, session))


async def urls_data(urls: Union[list | str], key: str, session: ClientSession) -> str:
    """Функция для объединения данных после отправки запросов по urls"""
    result = []
    if urls:
        async for item in tasks_urls(urls, key, session):
            result.append(item)
        return ", ".join(result)
    return ""


async def insert_to_db(people: list[dict]) -> None:
    """Функция для вставки данных в БД"""
    async with Session() as db_session:
        async with ClientSession() as session:
            for data in people:
                if data.get("status") == 404:
                    break
                person = SwapiPeople(
                    birth_year=data.get("birth_year"),
                    eye_color=data.get("eye_color"),
                    films=await urls_data(data.get("films"), "title", session),
                    gender=data.get("gender"),
                    hair_color=data.get("hair_color"),
                    height=data.get("height"),
                    homeworld=await urls_data(data.get("homeworld"), "name", session),
                    mass=data.get("mass"),
                    name=data.get("name"),
                    skin_color=data.get("skin_color"),
                    species=await urls_data(data.get("species"), "name", session),
                    starships=await urls_data(data.get("starships"), "name", session),
                    vehicles=await urls_data(data.get("vehicles"), "name", session),
                )
                db_session.add(person)
                await db_session.commit()


async def main():
    await init_db()
    session = ClientSession()
    for people_id_chunk in chunked(range(1, PEOPLE_CNT), MAX_CHUNK):
        coros = [get_person(person_id, session) for person_id in people_id_chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_to_db(result))
    await session.close()

    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)


if __name__ == "__main__":
    start = datetime.now()
    asyncio.run(main())
    print(f"Все данные помещены в БД. Время выполнения: {datetime.now() - start}")
