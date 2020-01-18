from flask import Flask
from db_utils import load_last_data, load_last_record_for_each_bank
import json

DATA_EXAMPLE = [
    {'name': 'БАНК КРЕМЛЕВСКИЙ ОКВКУ Аврора',
     'phone': '+7 903 575-11-83',
     'buy': '64.50', 'sell': '61.50',
     'time': '23:34', 'address': 'ул. Петровка, дом 11',
     'location': [55.7558, 37.6273]},
    {'name': 'БАНК КРЕМЛЕВСКИЙ ОКВКУ Лесная',
     'phone': '+7 968 534-76-17',
     'buy': '64.70', 'sell': '61.50', 'time': '23:34', 'address': 'Улица Лесная, д. 15',
     'location': [55.7558, 37.6473]}]


def process_request():
    all_data = []
    for city in (1, 2):
        for currency in (2, 3):
            data = load_last_record_for_each_bank(city, currency)
            last_data = load_last_data(city, currency)

            last_data_names = [item['name'] for item in last_data]

            for item in data:
                if item['name'] in last_data_names:
                    item['is_open'] = True
                else:
                    item['is_open'] = False

            all_data.extend(data)
    return all_data


app = Flask(__name__)
app.debug = True


@app.route('/load', methods=['GET'])
def calculate_method():
    data = process_request()

    response = app.response_class(
            response=json.dumps(data, ensure_ascii=False),
            status=200,
            mimetype='application/json')
    return response


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5555)
