import asyncio
import aiohttp
import datetime
from more_itertools import chunked


from models import init_db, Session, SwapiPeople

MAX_CHUNK = 5


async def get_person(person_id, session):
    http_response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    json_data = await http_response.json()
    return json_data


async def insert_records(records):
    records = [SwapiPeople(json=record) for record in records]
    async with Session() as session:
        session.add_all(records)
        await session.commit()


async def main():
    await init_db()
    async with aiohttp.ClientSession() as session:
        # разбиваем 90 запросов по 5 штук
        for people_id_chunk in chunked(range(1, 90), MAX_CHUNK):
            print(people_id_chunk)
            coros = [get_person(person_id, session) for person_id in people_id_chunk]
            result = await asyncio.gather(*coros)
            # await insert_records(result)
            # не блокируем выгрузку следующих данных из API, пока предыдущие помещаются в БД
            asyncio.create_task(insert_records(result))

    # await session.close()

    # нужно проверить, завершились ли все задачи
    # asyncio.all_tasks() - множество выполняемых задач, но туда попадает и задача с выполнением main()
    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks_set)  # вместо следующих двух строчек
    # for task in all_tasks_set:
    #     await task

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
