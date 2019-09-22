import mysql.connector


def parse_row(row):

    # id, load_id, timestamp, name, phone, time, buy, sell, address, longitude, latitude
    # (7, 2, datetime.datetime(2019, 9, 22, 19, 44, 1), 'АКБ Трансстройбанк ОКВКУ Братиславская',
    #  '+7 495 786-37-73 доб. 562', '20:43', 64.4, 63.8, 'ул. Братиславская, д. 14', 55.7558, 37.6273)
    #

    return {
        'name': row[3],
        'phone': row[4],
        'time': row[5],
        'buy': row[6],
        'sell': row[7],
        'address': row[8],
        'location': [row[9], row[10]]
    }


def insert_row(object, cursor):
    sql = f"""INSERT INTO office_rates (load_id, office_name, office_phone, time, buy_rate, sell_rate, address, longitude, latitude)""" \
        f""" VALUES ({object['load_id']}, '{object['name']}', '{object['phone']}', '{object['time']}', """ \
        f"""{object['buy_rate']}, {object['sell_rate']}, '{object['address']}', {object['longitude']}, {object['latitude']})"""
    cursor.execute(sql)


def update_db(results):
    conn = mysql.connector.connect(
            database="mvpdb",
            host="localhost",
            user="dbuser",
            passwd="12345",
            use_unicode=True,
            charset='utf8')

    # conn.set_charset_collation('utf-8')
    cursor = conn.cursor(buffered=True)

    _ = cursor.execute("SELECT MAX(load_id) from office_rates")
    last_load_id = cursor.fetchone()[0]

    for item in results:
        item['load_id'] = last_load_id + 1
        insert_row(item, cursor)

    conn.commit()
    conn.close()


def load_last_data():
    conn = mysql.connector.connect(
        database="mvpdb",
        host="localhost",
        user="dbuser",
        passwd="12345",
        use_unicode=True,
        charset='utf8')

    # conn.set_charset_collation('utf-8')
    cursor = conn.cursor(buffered=True)

    _ = cursor.execute("SELECT * from office_rates where load_id = (SELECT MAX(load_id) from office_rates)")
    res = []
    for row in cursor:
        res.append(parse_row(row))
    conn.close()
    return res

