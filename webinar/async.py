import asyncio
from pprint import pprint

import aiohttp
import datetime


async def get_person(person_id):
    session = aiohttp.ClientSession()
    coro = session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    result = await coro
    coro = result.json()
    result = await coro
    await session.close()
    return result


async def main():
    coro1 = get_person(1)
    coro2 = get_person(2)
    coro3 = get_person(3)
    coro4 = get_person(4)

    result = await asyncio.gather(coro1, coro2, coro3, coro4)
    pprint(result)


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
