import sqlite3

import create_table as ct
import insert_into_table as it

def get_connector():
    mydb = sqlite3.connect('genesis_db.db')
    return mydb

def create_table_(sql):
    conn = get_connector()
    c = conn.cursor()
    print("Executing ", sql)
    c.execute(sql)
    conn.commit()

def insert_data_into_table(sql):
    conn = get_connector()
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    c.close()

def insert_data_into_table(sql):
    conn = get_connector()
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    c.close()


def insert_prefed_sensor():
    sensor_list = ['VER_W1_B2_FF_B_1_temp','VER_W1_B2_FF_A_1_temp','VER_W1_WARLVL_WARLVL_WARLVL_1_Temp','VER_W1_B2_FF_B_1_rh','VER_W1_B2_GF_B_1_rh','VER_W1_B2_LG_A_1_rh','VER_W1_B2_SF_C_1_rh']
    for sensor in sensor_list:
        sql = it.add_sensor_to_sensor_master(sensor_name=sensor)
        insert_data_into_table(sql)

create_table_(ct.create_sensor_master)
create_table_(ct.create_history_table)
insert_prefed_sensor()


