import sqlite3
from influxdb_client import WritePrecision, Point, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import json
import pytz




class Influx_db_Test_bucket():
    def __init__(self):
        self.token = "I3e5igJmXNSNQmWHhPfLI6tDSeffnTnE3HzzFvHzJSrW1lEGGM8-59UN4jJfRKRXne0wf2veD8ijQOGo1fVSyQ=="
        self.org = "phAIdelta"
        self.bucket = "mydb"
        self.client = InfluxDBClient(url="http://localhost:8086", token=self.token, org=self.org)

    def read_data_(self, query):
        query_api = self.get_read_api()
        result = query_api.query(org=self.org, query = query)
        results = {}
        # results = {'sensor_data':[]}
        for table in result:
            for record in table.records:
                timestamp = str(record.get_time().timestamp())
                if record.get_field() not in results:
                    results[ record.get_field()]=[]
                try :
                    results[ record.get_field()].append({timestamp  : record.get_value()})
                except KeyError:
                    results[ record.get_field()]=[]
                    results[ record.get_field()].append({timestamp  : record.get_value()})
        return results

    def get_write_api(self):
        return self.client.write_api(write_options = SYNCHRONOUS)

    def get_read_api(self):
        return self.client.query_api()

    def write_data(self,pointer):
        self.write_api = self.get_write_api()
        self.write_api.write(bucket=self.bucket, org=self.org, record=pointer,write_precision=WritePrecision.S)
        self.write_api.close()

    def upload_data_to_history_table(self,data):
        print(data)
        if data['status'] == 'ok':
            p = Point('history_data').tag("location","VER_W1").field(data['ID'],data['VALUE']).time(datetime.utcfromtimestamp(float(data['timestamp'])), WritePrecision.NS)
            print("Writing data into history_data at location : VER_W1 ", type(datetime.utcfromtimestamp(float(data['timestamp']))))
            self.write_data(p)

influx_db = Influx_db_Test_bucket()





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
    return read_data_from_table(f"select * from history_table order by history_id DESC LIMIT 100")

def get_time_series_data():
    query = f' from(bucket:"{influx_db.bucket}")\
    |> range(start: 0)\
    |> filter(fn:(r) => r._measurement == "history_data")\
    |> filter(fn: (r) => r.location == "VER_W1")'
    result =  influx_db.read_data_(query)
    return result

# def insert_data_into_history_table(data):
#     sql =  f''' INSERT INTO history_table (sensor_name, sensor_value,status_tag,create_ts) VALUES ('{data['ID']}','{data['VALUE']}','{data['status']}','{data['timestamp']}')'''
#     insert_data_into_table(sql)

def insert_data_into_history_table(data):
    # print(data)
    influx_db.upload_data_to_history_table(data)

# influx_db.upload_data_to_history_table({"ID": "VER_W1_B2_FF_B_1_temp","VALUE": 25, "timestamp": datetime.now().timestamp(),"status": "ok"})

get_time_series_data()