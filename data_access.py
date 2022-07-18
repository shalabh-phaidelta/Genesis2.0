import sqlite3
from datetime import datetime
import pytz



class dotdict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

tz = pytz.timezone('Asia/Kolkata')


def get_results(db_cursor):
    desc = [d[0] for d in db_cursor.description]
    results = [dict(dotdict(dict(zip(desc, res)))) for res in db_cursor.fetchall()]
    return desc ,results

def get_connector():
    mydb = sqlite3.connect('genesis_db.db')
    return mydb

def insert_data_into_table(sql):
    conn = get_connector()
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    c.close()

def read_data_from_table(sql):
    conn = get_connector()
    c = conn.cursor()
    c.execute(sql)
    col , data = get_results(c)
    conn.commit()
    c.close()
    return col, data

def get_datetime_format(key, data):
    global tz
    for index in range(len(data)):
        data[index][key] = datetime.fromtimestamp(int(data[index][key]), tz)
    return data

def get_data_from_sensor_master(sensor_id = None):
    if sensor_id is None:
        data =read_data_from_table(f'select * from sensor_master')
    else:
       data = read_data_from_table(f'select * from sensor_master where sensor_id = {sensor_id}')
    return data

def get_last_100_entry():
    colum, result =  read_data_from_table(f"select * from history_table order by history_id DESC LIMIT 100")
    result = get_datetime_format('sensor_ts',result)
    return colum, result


def get_data_between_time(start_time=datetime.now().timestamp()-10000, end_time=datetime.now().timestamp()):
    colum, result =  read_data_from_table(f"select * from history_table WHERE sensor_ts BETWEEN {start_time} AND {end_time} order by sensor_ts DESC limit 1000")
    result = get_datetime_format('sensor_ts',result)
    return colum, result

def insert_data_into_history_table(data):
    sql =  f''' INSERT INTO history_table (sensor_name, sensor_value,status_tag,sensor_ts,create_ts) VALUES ('{data['ID']}','{data['VALUE']}','{data['status']}','{data['timestamp']}',{datetime.now().timestamp()})'''
    insert_data_into_table(sql)



