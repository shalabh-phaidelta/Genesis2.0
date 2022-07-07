import sqlite3
import datetime



class dotdict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


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

def get_data_from_sensor_master(sensor_id = None):
    if sensor_id is None:
        data =read_data_from_table(f'select * from sensor_master')
    else:
       data = read_data_from_table(f'select * from sensor_master where sensor_id = {sensor_id}')
    return data

def get_history_data():
    return read_data_from_table(f"select * from history_table")

def insert_data_into_history_table(data):
    sql =  f''' INSERT INTO history_table (sensor_name, sensor_value,status_tag,create_ts) VALUES ('{data['ID']}','{data['VALUE']}','{data['status']}','{data['timestamp']}')'''
    insert_data_into_table(sql)



