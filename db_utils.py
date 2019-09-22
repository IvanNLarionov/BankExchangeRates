import mysql.connector


def insert_row(object, cursor):
    sql = f"""INSERT INTO office_rates (load_id, office_name, office_phone, time, buy_rate, sell_rate, address, longitude, latitude)""" \
        f""" VALUES ({object['load_id']}, '{object['office_name']}', '{object['office_phone']}', '{object['time']}', """ \
        f"""{object['buy_rate']}, {object['sell_rate']}, '{object['address']}', {object['longitude']}, {object['latitude']})"""
    cursor.execute(sql)


def update_db(results):
    conn = mysql.connector.connect(
            database="mvpdb",
            host="localhost",
            user="dbuser",
            passwd="12345")

    cursor = conn.cursor(buffered=True)

    _ = cursor.execute("SELECT MAX(load_id) from office_rates")
    last_load_id = cursor.fetchone()[0]

    for item in results:
        item['load_id'] = last_load_id + 1
        insert_row(item, cursor)

    conn.commit()
    conn.close()
