import asyncio
import aiohttp
import requests
from db_utils import update_db
from scrape_utils import *
import logging


async def run(city, currency):

    url = 'https://cash.rbc.ru/cash/'
    payload = {'city': city, 'currency': currency, 'amount': 1000}
    response = requests.get(url, params=payload)
    soup = BeautifulSoup(response.text, 'html.parser')

    tasks = []

    offices = get_offices(soup)
    async with aiohttp.ClientSession() as session:
        async with aiohttp.ClientSession() as yandex_session:
            for office in offices:
                task = asyncio.ensure_future(create_office_record(office, city, currency, session, yandex_session))
                tasks.append(task)

            results = await asyncio.gather(*tasks)
    results = [item for item in results if item['latitude'] != 'nan' and item['longitude'] != 'nan']
    for result in results:
        result['latitude'] = float(result['latitude'])
        result['longitude'] = float(result['longitude'])
    print(len(results))
    print(results)
    update_db(results, city, currency)
    return results


### city = 1 -> Moscow
### city = 2 -> SPB
### currency = 3 -> USD
### currency = 2 -> EUR

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    for city in (1, 2):
        for currency in (2, 3):
            loop.run_until_complete(run(city=city, currency=currency))

