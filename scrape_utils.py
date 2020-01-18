import json
import logging
import aiohttp

from bs4 import BeautifulSoup

OFFICE_CLASS = "quote__office__one js-one-office"
NAME_CLASS = "quote__office__one__name"
BUY_CLASS = "quote__office__cell quote__office__one__buy js-filter-pro quote__mode_list_view_pro"
SELL_CLASS = "quote__office__cell quote__office__one__sell js-filter-pro quote__mode_list_view_pro"
PHONE_CLASS = "quote__office__one__phone"
TIME_CLASS = "quote__office__cell quote__office__one__time"

BASE_URL = 'https://cash.rbc.ru'

API_KEY = "32566061-1bdc-4f0a-80a5-d194c236c20a"
YANDEX_URL = "https://geocode-maps.yandex.ru/1.x"


def get_offices(soup):
    return soup.findAll("div", {"class": OFFICE_CLASS})


def get_office_name(office_soup):
    return office_soup.find("a", {"class": NAME_CLASS}).text


def get_office_ref(office_soup):
    return BASE_URL + office_soup.find("a", {"class": NAME_CLASS})['href']


def get_phone(office_soup):
    return office_soup.find("div", {"class": PHONE_CLASS}).text


def get_buy_rate(office_soup):
    return list(office_soup.find("div", {"class": BUY_CLASS}).stripped_strings)[0]


def get_sell_rate(office_soup):
    return list(office_soup.find("div", {"class": SELL_CLASS}).stripped_strings)[0]


def get_record_time(office_soup):
    return office_soup.find("div", {"class": TIME_CLASS}).text.replace("\n", "").replace(" ", "")


async def fetch_inner(office_ref, session):
    async with session.get(office_ref) as response:
        text = await response.text()
        try:
            soup = BeautifulSoup(text, 'html.parser')
            address = soup.find("div", {"class": "quote__info__address"}).text
            address = address.replace("Адрес: ", "")
            return address
        except Exception as e:
            logging.debug(e.args)
            return None


async def fetch_geoloc(address, city, yandex_session):
    url = YANDEX_URL

    if city == 1:
        city_name = "Москва"
        bbox = "35.5,54.5~38.5,57.5"
    elif city == 2:
        city_name = "Санкт-Петербург"
        bbox = "28.8,58.43~31.8,61.43"
    else:
        raise ValueError("Working with moscow and spb currently only")

    address_for_geocode = city_name + ' ' + address

    data = {'geocode': address_for_geocode,
            'apikey': API_KEY,
            'format': 'json',
            "rspn": 1,
            "bbox": bbox,
            }

    async with yandex_session.get(url=url, params=data) as response:
        res = await response.json()
        results = res['response']['GeoObjectCollection']['featureMember']
        if len(results) > 1:
            print("Yandex geoloc received multiple results")
            print(results)
        location = results[0]['GeoObject']['Point']['pos']
        location = str.split(location, " ")
        location = [float(item) for item in location]
    return location


async def create_office_record(office_soup, city, currency, session, yandex_session):
    name = get_office_name(office_soup)
    phone = get_phone(office_soup)
    buy_rate = get_buy_rate(office_soup)
    sell_rate = get_sell_rate(office_soup)
    time = get_record_time(office_soup)

    inner_ref = get_office_ref(office_soup)
    address = await fetch_inner(inner_ref, session)
    try:
        coordinates = await fetch_geoloc(address, city, yandex_session)
    except Exception as e:
        print(f"Failed fetching coordinates for address {address} in city {city}")
        print(e.args)
        coordinates = ('nan', 'nan')

    return {'name': name,
            'phone': phone,
            'buy_rate': buy_rate,
            'sell_rate': sell_rate,
            'time': time,
            'address': address,
            'city': city,
            'currency': currency,
            'latitude': coordinates[0],
            'longitude': coordinates[1]}
