import pandas as pd

from . import models as db
from . import vw_table as vw

def get_connector():
    conn = db.engine.connect()
    return conn

def get_data_from_metric(metric_id, sensor_type):
    try:
        sensor_dict = {'Temp': 'temp', 'Pressure':'pres'}
        df = pd.DataFrame(pd.read_sql_query(f'select * from vw_{sensor_dict[sensor_type]}_summary_unit where sensor_id  = {metric_id}', get_connector()), columns=['sensor_data_calc_ts', 'metric_name', 'max_15minutes', 'avg_15_minutes', 'min_15minutes'])
        if not df.empty:
            return df 
        else:
            return None
    except db.exc.NoResultFound :
        return None
