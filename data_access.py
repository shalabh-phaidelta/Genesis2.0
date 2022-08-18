import sqlite3
from datetime import datetime
import pytz

from shared import logger as log


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
    log.logger("Inserting data into history table")
    insert_data_into_table(sql)

def recive_data_from_aliter(info):
    try:
        slave_id = get_slave_id(info['device_name'])
        log.logger(f"Recived data from Aliter for slave id : {slave_id}")
    except IndexError:
        log.logger(f"Device {info['device_name']} doesn't exist in database","Error")
        return None

    for item in info['data']:
        data={}
        if item == "slave_id" or item =="function_code":
            pass
        else:
            mem = item.split('_')[-1]
            try:
                data['ID']=get_parameter_name(slave_id, mem)
            except IndexError:
                log.logger(f"Sensor Does not exist for {item} in slave {slave_id}","Error")
                continue
    
            data['VALUE'] = info['data'][item]
            data['status'] = 'ok'
            data['timestamp'] = info['ts']
            insert_data_into_history_table(data)
            log.logger(f"Inserted {data['ID']} into table successfully")



def get_device_id(mac):
    log.logger("Fetching device id")
    result = read_data_from_table(f"select device_id from device_master where mac_address = '{mac}' ")[1][0]['device_id']
    return result

def get_slave_id(slave_name):
    log.logger("Fetching slave id")
    result = read_data_from_table(f"select slave_id from slave_master where slave_name = '{slave_name}' ")[1][0]['slave_id']
    return result

def get_parameter_name(slave_id,memory):
    log.logger("Fetching Sensor name")
    result = read_data_from_table(f"SELECT sensor_name FROM  vw_sensor_slave_maping sm WHERE parameter_address = {memory} AND  slave_id ={slave_id}")[1][0]['sensor_name']
    return result

    
