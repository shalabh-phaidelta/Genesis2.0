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
    sql =  f''' INSERT INTO history_table (sensor_name, sensor_value,status_tag,sensor_ts,create_ts) VALUES ('{data['ID']}','{data['VALUE']}','{data['status']}','{data['timestamp']}',{int(datetime.now().timestamp())})'''
    insert_data_into_table(sql)

def recive_data_from_aliter(info):
    slave_id = get_slave_id(info['device_name'])
    for item in info['data']:
        data={}
        if item == "slave_id" or item =="function_code":
            pass
        else:
            mem = item.split('_')[-1]
            data['ID']=get_parameter_name(slave_id, mem)
            data['VALUE'] = info['data'][item]
            data['status'] = 'ok'
            data['timestamp'] = info['ts']
            insert_data_into_history_table(data)



def get_device_id(mac):
    result = read_data_from_table(f"select device_id from device_master where mac_address = '{mac}' ")[1][0]['device_id']
    return result

def get_slave_id(slave_name):
    result = read_data_from_table(f"select slave_id from slave_master where slave_name = '{slave_name}' ")[1][0]['slave_id']
    return result

def get_parameter_name(slave_id,memory):
    result = read_data_from_table(f"SELECT sensor_name FROM  vw_sensor_slave_maping sm WHERE parameter_address = {memory} AND  slave_map ={slave_id}")[1][0]['sensor_name']
    return result

    
