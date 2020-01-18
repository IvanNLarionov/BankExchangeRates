import mysql.connector
import logging
import pandas as pd
logger = logging.getLogger()


TABLE_NAME = "office_rates"

def parse_row(row):
    # id, load_id, timestamp, name, phone, time, buy, sell, address, longitude, latitude
    # (7, 2, datetime.datetime(2019, 9, 22, 19, 44, 1), 'АКБ Трансстройбанк ОКВКУ Братиславская',
    #  '+7 495 786-37-73 доб. 562', '20:43', 64.4, 63.8, 'ул. Братиславская, д. 14', 55.7558, 37.6273)
    #
    return {
        'load_timestamp': pd.to_datetime(row[2]).timestamp(),
        'name': row[3],
        'phone': row[4],
        'time': row[5],
        'buy': row[6],
        'sell': row[7],
        'address': row[8],
        'location': [row[9], row[10]],
        'currency': row[11],
        'city': row[12],
    }


def insert_row(object, cursor):
    sql = f"""INSERT INTO {TABLE_NAME} 
            (load_id, office_name, office_phone, time,
             buy_rate, sell_rate, address,
             longitude, latitude, currency, city)""" \
        f""" VALUES
         ({object['load_id']}, '{object['name']}', '{object['phone']}', '{object['time']}', 
         {object['buy_rate']}, {object['sell_rate']}, '{object['address']}',
         {object['longitude']}, {object['latitude']}, {object['currency']}, {object['city']})"""
    cursor.execute(sql)
    print(f"inserted object {object}")


def update_db(results, city, currency):
    conn = mysql.connector.connect(
            database="mvpdb",
            host="localhost",
            user="dbuser",
            passwd="12345",
            use_unicode=True,
            charset='utf8')

    cursor = conn.cursor(buffered=True)

    _ = cursor.execute(f"""SELECT MAX(load_id) from {TABLE_NAME} where city = {city} and currency ={currency}""")
    last_load_id = cursor.fetchone()[0]
    print(f"last load id is {last_load_id}")
    if last_load_id:
        new_load_id = last_load_id + 1
    else:
        new_load_id = 1
    for item in results:
        item['load_id'] = new_load_id
        insert_row(item, cursor)

    conn.commit()
    conn.close()


def load_last_data(city, currency):
    conn = mysql.connector.connect(
        database="mvpdb",
        host="localhost",
        user="dbuser",
        passwd="12345",
        use_unicode=True,
        charset='utf8')

    # conn.set_charset_collation('utf-8')
    cursor = conn.cursor(buffered=True)
    query = f"""SELECT * from {TABLE_NAME} 
    where load_id = (SELECT MAX(load_id) from {TABLE_NAME} and city = {city} and currency = {currency}"""

    _ = cursor.execute(query)
    res = []
    for row in cursor:
        res.append(parse_row(row))
    conn.close()
    return res


def load_last_record_for_each_bank(city, currency):
    conn = mysql.connector.connect(
        database="mvpdb",
        host="localhost",
        user="dbuser",
        passwd="12345",
        use_unicode=True,
        charset='utf8')

    # conn.set_charset_collation('utf-8')
    cursor = conn.cursor(buffered=True)

    query = f"""select * from {TABLE_NAME} 
    where 
        id in (SELECT MAX(id) from {TABLE_NAME}
         where city = {city} and currency={currency} group by office_name)"""
    _ = cursor.execute(query)

    res = []
    for row in cursor:
        res.append(parse_row(row))
    conn.close()
    return res


