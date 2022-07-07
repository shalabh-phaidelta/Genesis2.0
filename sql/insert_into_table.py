import datetime



def add_sensor_to_sensor_master(sensor_name, sensor_alias=None,created_by='Admin',create_ts=datetime.datetime.now().timestamp()):
    if sensor_alias is not None:
        sql = f'''
            INSERT INTO sensor_master (sensor_name, sensor_alias,created_by,create_ts)

                    VALUES
                    ('{sensor_name}', {sensor_alias},'{created_by}',{create_ts})
            '''
    else:
        sql = f'''
            INSERT INTO sensor_master (sensor_name, created_by,create_ts)
                    VALUES
                    ('{sensor_name}', '{created_by}',{create_ts})
            '''
    print(sql)
    return sql