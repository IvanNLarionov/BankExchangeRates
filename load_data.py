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

    tasks = []

    offices = get_offices(soup)
    async with aiohttp.ClientSession() as session:
        async with aiohttp.ClientSession() as yandex_session:
            for office in offices:
                task = asyncio.ensure_future(create_office_record(office, session, yandex_session))
                tasks.append(task)

            results = await asyncio.gather(*tasks)
    results = [item for item in results if item['latitude'] != 'nan' and item['longitude'] != 'nan']
    for result in results:
        result['latitude'] = float(result['latitude'])
        result['longitude'] = float(result['longitude'])
    print(len(results))
    print(results)
    update_db(results)
    return results


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
