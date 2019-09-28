import logging
from yandex_geocoder import Client

from bs4 import BeautifulSoup

OFFICE_CLASS = "quote__office__one js-one-office"
NAME_CLASS = "quote__office__one__name"
BUY_CLASS = "quote__office__cell quote__office__one__buy js-filter-pro quote__mode_list_view_pro"
SELL_CLASS = "quote__office__cell quote__office__one__sell js-filter-pro quote__mode_list_view_pro"
PHONE_CLASS = "quote__office__one__phone"
TIME_CLASS = "quote__office__cell quote__office__one__time"

BASE_URL = 'https://cash.rbc.ru'


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


async def create_office_record(office_soup, session):
    name = get_office_name(office_soup)
    phone = get_phone(office_soup)
    buy_rate = get_buy_rate(office_soup)
    sell_rate = get_sell_rate(office_soup)
    time = get_record_time(office_soup)

    inner_ref = get_office_ref(office_soup)
    address = await fetch_inner(inner_ref, session)

    address_for_coordinates = "Москва " + address
    try:
        coordinates = Client.coordinates(address_for_coordinates)
    except Exception as e:
        print(f"Failed fetching coordinates for address {address_for_coordinates}")
        print(e.args)
        coordinates = ('NaN', 'NaN')

    return {'name': name,
            'phone': phone,
            'buy_rate': buy_rate,
            'sell_rate': sell_rate,
            'time': time,
            'address': address,
            'latitude': float(coordinates[0]),
            'longitude': float(coordinates[1])}
