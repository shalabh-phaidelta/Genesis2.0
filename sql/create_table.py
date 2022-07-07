
create_sensor_master= '''
CREATE TABLE IF NOT EXISTS sensor_master
([sensor_id] INTEGER PRIMARY KEY AUTOINCREMENT,
[sensor_name] TEXT,
[sensor_alias] TEXT,
[created_by] TEXT DEFAULT "Admin" NOT NULL,
[create_ts] INTEGER DEFAULT CURRENT_TIMESTAMP,
[updated_by] TEXT,
[update_ts] INTEGER)
'''


create_history_table= '''
CREATE TABLE IF NOT EXISTS history_table
([history_id] INTEGER PRIMARY KEY AUTOINCREMENT,
[sensor_name] TEXT,
[sensor_value] INTEGER,
[create_ts] INTEGER,
[status_tag] TEXT)
'''

