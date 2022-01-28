#===================================================================================================================================#
# Header
#===================================================================================================================================#

from flask import Flask, render_template, url_for, request, jsonify
import sqlite3
import requests
import datetime
import os
import json
import numpy as np
import time
import random

app = Flask(__name__)
app.static_folder = 'static'

def column_list(table, db_file):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute('PRAGMA table_info(' + table + ');')
    columns_data = cur.fetchall()
    conn.close()

    col_list = []
    it = 1
    for c in columns_data:
        col_list.append(c[1])
        it = it + 1
    return col_list

def get_entire_data(table, db_file):
    sql = ' SELECT * FROM ' + table + '; '
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    conn.close()

    return data

@app.route('/static/<filename>')
def cssstyle(filename):
    return url_for('static', filename='css/style.css', _external=True)

#===================================================================================================================================#
# Salary Conversion
#===================================================================================================================================#

JSON_PATH_CURRENCY = 'C:\\Users\\ASUS\\Documents\\PythonScripts\\seleksi\\cad-it\\JSON Files\\currency_data.json'
JSON_PATH_USERDATA = 'C:\\Users\\ASUS\\Documents\\PythonScripts\\seleksi\\cad-it\\JSON Files\\user_data.json'
JSON_PATH_SALARY = 'C:\\Users\\ASUS\\Documents\\PythonScripts\\seleksi\\cad-it\\JSON Files\\salary_data.json'
DB_FILE_SALARY = 'C:\\Users\\ASUS\\Documents\\PythonScripts\\seleksi\\cad-it\\user_salary_database.db'

URL_CURRENCY = 'https://free.currconv.com'

def salary_conversion():
    @app.route('/salary_conversion/tables/<table>')
    def index_salary(table):
        header_text = table + ' table'
        col_list = column_list(table, DB_FILE_SALARY)
        data = get_entire_data(table, DB_FILE_SALARY)
        return render_template('index.html', header_text=header_text, col_list=col_list, data=data)

    @app.route('/salary_conversion/joined')
    def joined():
        header_text = 'Salary Conversion'
        col_list = ['ID', 'Name', 'Username', 'Email', 'Address', 'Phone', 'Salary in IDR', 'Salary in USD']
        data = join_table()
        return render_template('index.html', header_text=header_text, col_list=col_list, data=data)

    def fetch_user_data():
        resp = requests.get('https://jsonplaceholder.typicode.com/users')

        f = open(JSON_PATH_USERDATA, 'w')
        json.dump(resp.json(), f)
        f.close()

    def insert_user_data():
        resp = requests.get('https://jsonplaceholder.typicode.com/users')

        sql = ''' INSERT INTO users(name, username, email, address, phone) VALUES (?, ?, ?, ?, ?); '''
        conn = sqlite3.connect(DB_FILE_SALARY)
        cur = conn.cursor()
        for i in resp.json():
            address = i['address']['street'] + ', ' + i['address']['suite'] + ', '+ i['address']['city'] + ', ' + i['address']['zipcode']
            cur.execute(sql, (i['name'], i['username'], i['email'], address, i['phone']))
        conn.commit()
        conn.close()

    def create_user_table():
        sql = """ CREATE TABLE IF NOT EXISTS users(
            id integer PRIMARY KEY,
            name text,
            username text,
            email text,
            address text,
            phone text
            );"""

        conn = sqlite3.connect(DB_FILE_SALARY)
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS users;')
        cur.execute(sql)
        conn.close()

    def create_salary_table():
        sql = """ CREATE TABLE IF NOT EXISTS salaries(
            id integer PRIMARY KEY,
            salary_in_idr integer,
            salary_in_usd integer,
            user_id integer,
            FOREIGN KEY(user_id) REFERENCES users(id)
            );"""

        conn = sqlite3.connect(DB_FILE_SALARY)
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS salaries;')
        cur.execute(sql)
        conn.close()

    def insert_salary_data():
        f = open(JSON_PATH_SALARY)
        json_salary = json.load(f)
        f.close()

        json_currency = currency_data()

        sql = ''' INSERT INTO salaries(salary_in_idr, salary_in_usd, user_id) VALUES (?, ?, ?); '''
        conn = sqlite3.connect(DB_FILE_SALARY)
        cur = conn.cursor()
        for i in json_salary['array']:
            salaryInIDR = i['salaryInIDR']
            salaryInUSD = salaryInIDR * json_currency['IDR-USD']
            user_id = i['id']
            cur.execute(sql, (round(salaryInIDR), round(salaryInUSD), user_id))
        conn.commit()
        conn.close()

    def join_table():
        sql = ''' SELECT users.id, users.name, users.username, users.email, users.address, users.phone,         salaries.salary_in_idr, salaries.salary_in_usd
                FROM salaries
                INNER JOIN users
                ON users.id = salaries.user_id;
                '''

        conn = sqlite3.connect(DB_FILE_SALARY)
        cur = conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        conn.close()

        return data

    def currency_data():
        json_data = {}
        if os.path.exists(JSON_PATH_CURRENCY):
            f = open(JSON_PATH_CURRENCY)

            json_data = json.load(f)

            f.close()
            
            date_now = datetime.datetime.utcnow().strftime('%d')
            if date_now != json_data['data']['date']:
                resp = requests.get(URL_CURRENCY + '/api/v7/convert?q=USD_IDR,IDR_USD&compact=ultra&apiKey=23057c5b39052b7a225e')
                print('Requesting GET Method')
                json_data['data'] = {'date': date_now, 'USD-IDR': resp.json()['USD_IDR'], 'IDR-USD': resp.json()['IDR_USD']}

                f = open(JSON_PATH_CURRENCY, 'w')
                json.dump(json_data, f)
            f.close()

        else:
            date_now = datetime.datetime.utcnow().strftime('%d')
            resp = requests.get(URL_CURRENCY + '/api/v7/convert?q=USD_IDR,IDR_USD&compact=ultra&apiKey=23057c5b39052b7a225e')
            print('Requesting GET Method')
            json_data['data'] = {'date': date_now, 'USD-IDR': resp.json()['USD_IDR'], 'IDR-USD': resp.json()['IDR_USD']}

            f = open(JSON_PATH_CURRENCY, 'w')
            json.dump(json_data, f)
            f.close()

        return {'USD-IDR': json_data['data']['USD-IDR'], 'IDR-USD': json_data['data']['IDR-USD']}

    fetch_user_data()
    create_user_table()
    create_salary_table()
    insert_user_data()
    insert_salary_data()

#===================================================================================================================================#
# Sensors Aggregation
#===================================================================================================================================#

JSON_PATH_SENSOR = 'C:\\Users\\ASUS\\Documents\\PythonScripts\\seleksi\\cad-it\\JSON Files\\sensor_data.json'
DB_FILE_SENSOR = 'C:\\Users\\ASUS\\Documents\\PythonScripts\\seleksi\\cad-it\\sensor_database.db'

def sensors_aggregation():
    @app.route('/sensor_aggregation/tables/<table>')
    def index_sensor(table):
        header_text = table + ' table'
        col_list = column_list(table, DB_FILE_SENSOR)
        data = get_entire_data(table, DB_FILE_SENSOR)
        return render_template('index.html', header_text=header_text, col_list=col_list, data=data)

    @app.route('/sensor_aggregation/statistics')
    def stats():
        header_text = 'Sensor Aggregation'
        rooms = get_room_names()
        data = []
        for i in rooms:
            data_dict = {}
            data_dict['room'] = i
            data_dict['parameter'] = ['Temperature', 'Humidity']
            data_dict['min'] = [np.min(get_sensor_values(i, 'temperature')), np.min(get_sensor_values(i, 'humidity'))]
            data_dict['max'] = [np.max(get_sensor_values(i, 'temperature')), np.max(get_sensor_values(i, 'humidity'))]
            data_dict['median'] = [np.median(get_sensor_values(i, 'temperature')), np.median(get_sensor_values(i, 'humidity'))]
            data_dict['average'] = [np.average(get_sensor_values(i, 'temperature')), np.average(get_sensor_values(i, 'humidity'))]
            data.append(data_dict)
        return render_template('stats.html', header_text=header_text, data=data)

    def create_sensor_table(room):
        sql = """ CREATE TABLE IF NOT EXISTS """ + room + """(
            id integer PRIMARY KEY,
            timestamp text,
            temperature integer,
            humidity integer
            );"""

        conn = sqlite3.connect(DB_FILE_SENSOR)
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS ' + room + ';')
        cur.execute(sql)
        conn.close()

    def insert_sensor_data(room):
        f = open(JSON_PATH_SENSOR)
        data = json.load(f)
        f.close()

        sql = ''' INSERT INTO ''' + room + '''(timestamp, temperature, humidity) VALUES (?, ?, ?); '''
        conn = sqlite3.connect(DB_FILE_SENSOR)
        cur = conn.cursor()
        for i in data['array']:
            if i['roomArea'] == room:
                cur.execute(sql, (i['timestamp'], i['temperature'], i['humidity']))
        conn.commit()
        conn.close()

    def get_room_names():
        f = open(JSON_PATH_SENSOR)
        data = json.load(f)
        f.close()

        room_list = []

        for i in data['array']:
            if not room_list.__contains__(i['roomArea']):
                room_list.append(i['roomArea'])
        
        return room_list

    def get_sensor_values(room, parameter):
        sql = ''' SELECT ''' + parameter + ''' FROM ''' + room + ''';'''

        conn = sqlite3.connect(DB_FILE_SENSOR)
        cur = conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        conn.close()

        return data

    room_lists = get_room_names()

    for i in room_lists:
        create_sensor_table(i)
        insert_sensor_data(i)
#===================================================================================================================================#
# Sensors Aggregation Simulation
#===================================================================================================================================#

JSON_PATH_SIM = 'C:\\Users\\ASUS\\Documents\\PythonScripts\\seleksi\\cad-it\\JSON Files\\simulation_data.json'

Tx = [0]
Ty = [0]
Hx = [0]
Hy = [0]

MAX_LENGTH = 15

def sensors_aggregation_simulation():
    @app.route('/simulation')
    def sim():
        return render_template('simulation.html')

    @app.route('/simulation/values')
    def get_values():
        data = {}
        to_file = {}
        date_now = datetime.datetime.utcnow()
        data['temperature'] = {
            'x': date_now,
            'y': random.randint(20, 40)
        }
        data['humidity'] = {
            'x': date_now,
            'y': random.randint(80, 100)
        }
        if len(Tx) > MAX_LENGTH:
            Tx.pop(0)
            Ty.pop(0)
            Hx.pop(0)
            Hy.pop(0)
        Tx.append(data['temperature']['x'])
        Ty.append(data['temperature']['y'])
        Hx.append(data['humidity']['x'])
        Hy.append(data['humidity']['y'])
        to_file['array'] = []
        for i in range(0, len(Tx)):
            dict_data = {
                'temperature': Ty[i],
                'humidity': Hy[i],
                'timeStamp': date_now.strftime('%d/%m/%y, %H:%M:%S')
            }
            to_file['array'].append(dict_data)
        f = open(JSON_PATH_SIM, 'w')
        json.dump(to_file, f)
        f.close()

        data['min'] = {
            'temperature': int(np.min(Ty)),
            'humidity': int(np.min(Hy))
        }
        data['max'] = {
            'temperature': int(np.max(Ty)),
            'humidity': int(np.max(Hy))
        }
        data['median'] = {
            'temperature': round(float(np.median(Ty)), 2),
            'humidity': round(float(np.median(Hy)), 2)
        }
        data['average'] = {
            'temperature': round(float(np.average(Ty)), 2),
            'humidity': round(float(np.average(Hy)), 2)
        }
        data['maxlength'] = MAX_LENGTH
        return jsonify(data)


#===================================================================================================================================#
# Execution
#===================================================================================================================================#

salary_conversion()
sensors_aggregation()
sensors_aggregation_simulation()

if __name__ == '__main__':
    app.run(debug=True)


