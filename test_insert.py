import mysql.connector

conn = mysql.connector.connect(
        database="mvpdb",
        host="localhost",
        user="dbuser",
        passwd="12345")

cursor = conn.cursor(buffered=True)
cursor.execute("SHOW COLUMNS FROM office_rates")

object = {'load_id': 0,
          'office_name': 'test',
          'office_phone': '+79858885090',
          'time': '15:43',
          'buy_rate': 61.50,
          'sell_rate': 64.40,
          'address': "some address"}

sql = f"""INSERT INTO office_rates (load_id, office_name, office_phone, time, buy_rate, sell_rate, address)""" \
    f""" VALUES ({object['load_id']}, '{object['office_name']}', '{object['office_phone']}', '{object['time']}', """ \
    f"""{object['buy_rate']}, {object['sell_rate']}, '{object['address']}')"""

cursor.execute(sql)
conn.commit()

