import asyncio
import aiohttp
import requests

from db_utils import update_db
from scrape_utils import *
import logging


async def run():
    url = 'https://cash.rbc.ru/cash/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    logger = logging.getLogger()
    tasks = []

    offices = get_offices(soup)

    async with aiohttp.ClientSession() as session:
        for office in offices:
            task = asyncio.ensure_future(create_office_record(office, session))
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        print(len(results))
        print(results)

    update_db(results)
    return results


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
