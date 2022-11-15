
import pandas as pd
import os

from ....data_access import db_queries

# from wm_server.data_access import db_queries

from . import config
from . import utils

class DataAccess:
    """
    DB Contract class
    """
    df = None
    metadata = None
    fromtime = None
    totime = None
    
    #List of columns in each table, which might change for each metric
    #(hence it is put here instead of config.py)
    COLUMN_METRIC_ID = "sensor_id"
    COLUMN_METRIC_NAME = "global_sensor_name"
    COLUMN_METRIC_ALIAS = "sensor_alias"
    COLUMN_METRIC_LOCATION_ID = "location_id"
    COLUMN_METRIC_LOCATION_NAME = "global_location_name"
    COLUMN_METRIC_LOCATION_ALIAS = "location_alias"
    COLUMN_METRIC_UNIT_ID = "unit_id"
    COLUMN_METRIC_UNIT_NAME = "global_unit_name"
    COLUMN_METRIC_UNIT_ALIAS = "unit_alias"
    COLUMN_METRIC_TYPE = "sensor_type"
    COLUMN_METRIC_SUBTYPE = "sensor_subtype"
    COLUMN_METRIC_RDG_TIME = "measure_time"
    COLUMN_METRIC_RDG_RAW_VALUE = "temp_value"         #Depends on metric
    COLUMN_METRIC_RDG_VALUE = "temp_avg_15min"         #Depends on metric
    COLUMN_METRIC_RDG_VALUE_30M = "temp_avg_30min"
    COLUMN_METRIC_RDG_VALUE_60M = "temp_avg_60min"
    COLUMN_METRIC_RDG_VALUE_24H = "temp_avg_24h"
    COLUMN_METRIC_RDG_VALUE_MTD = "temp_avg_mtd"
    COLUMN_METRIC_UPPER_CROSS = "over_threshold"
    COLUMN_METRIC_LOWER_CROSS = "under_threshold"
    COLUMN_METRIC_UPPER_THRES = "upper_limit"
    COLUMN_METRIC_LOWER_THRES = "lower_limit"
    COLUMN_METRIC_NONAGG_MIN = "temp_min_15minutes"    #Depends on metric
    COLUMN_METRIC_NONAGG_MAX = "temp_max_15minutes"    #Depends on metric
    COLUMN_METRIC_IS_WHL_MTR = "is_warehouse_level_unit"
    
    COLUMN_POWER_SENSOR_ID = "sensor_id"
    COLUMN_POWER_METRIC_TYPE = "metrictype"
    COLUMN_POWER_METRIC_SUBTYPE = "metricsubtype"
    COLUMN_POWER_SENSOR_NAME = "sensor_name"
    COLUMN_POWER_SENSOR_ALIAS = "sensor_alias"
    COLUMN_POWER_METRIC_VALUE = "value"
    COLUMN_POWER_METRIC_UNIT = "unit"
    COLUMN_POWER_METRIC_STATE = "state"
    COLUMN_POWER_READING_ASOFDATE = "asofdatetime"
    COLUMN_POWER_LOCATION_ID = "location_id"
    COLUMN_POWER_LOCATION_NAME = "location_name"
    COLUMN_POWER_LOCATION_ALIAS = "location_alias"
    COLUMN_POWER_UNIT_IS_WAREHOUSE = "is_warehouselevel"
    
    COLUMN_ALERT_TIME = 'alert_opened_ts'
    COLUMN_ALERT_ID = 'all_alert_id'
    COLUMN_ALERT_LOCATION_ID = 'location_id'
    COLUMN_ALERT_LOCATION_NAME = 'global_location_name'
    COLUMN_ALERT_LOCATION_ALIAS = 'location_alias'
    COLUMN_ALERT_UNIT_ID = 'unit_id'
    COLUMN_ALERT_UNIT_NAME = 'global_unit_name'
    COLUMN_ALERT_UNIT_ALIAS = 'unit_alias'
    COLUMN_ALERT_CLIENT = 'role' #HERE ALSO
    COLUMN_ALERT_ETA_CHANGE_REASON = "alert_eta_change_reason"
    COLUMN_ALERT_RECT_ACTION_CHANGE_REASON = "alert_rect_action_change_reason"
    COLUMN_ALERT_METRIC_ID = 'sensor_id'
    COLUMN_ALERT_METRIC_NAME = 'global_sensor_name'
    COLUMN_ALERT_METRIC_ALIAS = 'sensor_alias'
    COLUMN_ALERT_METRIC_TYPE = 'alert_type'
    COLUMN_ALERT_METRIC_VAL = 'actual_value'
    COLUMN_ALERT_METRIC_TARGET = 'target_value'
    COLUMN_ALERT_METRIC_STATUS = 'alert_status'
    COLUMN_ALERT_METRIC_ACKBY = 'alert_acknowledged_by_user_id'
    COLUMN_ALERT_METRIC_ACKBY_NAME = 'alert_acknowledged_by_user_name'
    COLUMN_ALERT_METRIC_RECTBY = 'alert_rectified_by_user_id'
    COLUMN_ALERT_METRIC_RECTBY_NAME = 'alert_rectified_by_user_name'
    COLUMN_ALERT_METRIC_ACKDT = 'alert_acknowledged_ts'
    COLUMN_ALERT_METRIC_RECTDT = 'alert_rectified_ts'
    COLUMN_ALERT_METRIC_EXPECTED_RECT = 'alert_expected_rectification_date'
    COLUMN_ALERT_METRIC_RECTHOW = 'alert_rectification_action'
    COLUMN_ALERT_METRIC_RECTHOW_ID = 'alerts_rectification_action_id'
    COLUMN_ALERT_CATEGORY = 'alert_category'
    COLUMN_ALERT_HAS_TARGET_VALUE = 'has_target_value'
    
    def loadMetricData(self, user_id, metric_id, fromtime=None, totime=None, bufferInterval=None):
        """
        Load data given metric id, along with some parameters
        """
        
        #Save this for later
        self.fromtime = fromtime
        self.totime = totime
        
        #Time range to fetch the data from, with some time buffer
        #If not specified, will be (+/-)1 day
        if bufferInterval == None: 
            bufferInterval = config.METRIC_INTERVAL_AGGREGATE['1D'] 
        else :
            bufferInterval = config.METRIC_INTERVAL_AGGREGATE[bufferInterval]
        fetch_fromTS = utils.dtToDB(fromtime - bufferInterval)
        fetch_toTS = utils.dtToDB(totime + bufferInterval)
        
        #Fetch data
        cursors = db_queries.metricview_metric_summary(user_id, metric_id, fetch_fromTS, fetch_toTS)
        print(cursors)
        #The above query returns a generator of
        #cursors (because, you guessed it, STPs can
        #return many tables). So we get the first cursor (if it exists)
        cursor = next(cursors)
        dt = cursor.fetchall()
        
        self.df = pd.DataFrame(dt)
        if len(self.df) > 0:
            self.df.columns = cursor.column_names
        
        #Metric metadata
        self.metadata = {
            'metric_type': None,
            'metric_subtype': None,
            'thresholds': None,
            'iswhlunit': 0,
            'metric_id': metric_id,
            'metric_name': "INV_INV_INV_INV_INV_INV_INV",
            'metric_alias': None,
            'location_id': None,
            'location_name': None,
            'location_alias': None,
            'unit_id': None,
            'unit_name': None,
            'unit_alias': None
        }
        
        if len(self.df) > 0:
            if "No data found" in self.df.iloc[0]: return
            
            mtr_global_id = self.df[self.COLUMN_METRIC_NAME].mode()
            mtr_alias = self.df[self.COLUMN_METRIC_ALIAS].mode()
            mtr_iswhl = self.df[self.COLUMN_METRIC_IS_WHL_MTR]
            
            mtr_type = self.df[self.COLUMN_METRIC_TYPE].mode()
            mtr_subtype = self.df[self.COLUMN_METRIC_SUBTYPE].mode()
            mtr_th_upper = self.df[self.COLUMN_METRIC_UPPER_THRES].mode()
            mtr_th_lower = self.df[self.COLUMN_METRIC_LOWER_THRES].mode()
            
            mtr_loc_id = self.df[self.COLUMN_METRIC_LOCATION_ID].mode()
            mtr_loc_name = self.df[self.COLUMN_METRIC_LOCATION_NAME].mode()
            mtr_loc_alias = self.df[self.COLUMN_METRIC_LOCATION_ALIAS].mode()
            
            mtr_unit_id = self.df[self.COLUMN_METRIC_UNIT_ID].mode()
            mtr_unit_name = self.df[self.COLUMN_METRIC_UNIT_NAME].mode()
            mtr_unit_alias = self.df[self.COLUMN_METRIC_UNIT_ALIAS].mode()
            
            self.metadata['thresholds'] = {"upper": None,"lower": None}
            
            if len(mtr_global_id) > 0: self.metadata['metric_name'] = mtr_global_id[0]
            if len(mtr_alias) > 0: self.metadata['metric_alias'] = mtr_alias[0]
            if len(mtr_type) > 0: self.metadata['metric_type'] = mtr_type[0]
            if len(mtr_subtype) > 0: self.metadata['metric_subtype'] = mtr_subtype[0]
            if len(mtr_iswhl) > 0: self.metadata['iswhlunit'] = mtr_iswhl[0]
            if len(mtr_th_upper) > 0: self.metadata['thresholds']["upper"] = mtr_th_upper[0]
            if len(mtr_th_lower) > 0: self.metadata['thresholds']["lower"] = mtr_th_lower[0]
            if len(mtr_loc_id) > 0: self.metadata['location_id'] = int(mtr_loc_id[0])
            if len(mtr_loc_name) > 0: self.metadata['location_name'] = mtr_loc_name[0]
            if len(mtr_loc_alias) > 0: self.metadata['location_alias'] = mtr_loc_alias[0]
            if len(mtr_unit_id) > 0: self.metadata['unit_id'] = int(mtr_unit_id[0])
            if len(mtr_unit_name) > 0: self.metadata['unit_name'] = mtr_unit_name[0]
            if len(mtr_unit_alias) > 0: self.metadata['unit_alias'] = mtr_unit_alias[0]
    
    
    def loadAlertsData(self, user_id, fromtime=None, totime=None, location_list=None):
        """
        Load alerts data and parse it
        """
        
        #Save this for later
        self.fromtime = fromtime
        self.totime = totime
        self.region_ids = location_list
        
        #Fetch data
        cursors = db_queries.alertview_alert_table(user_id)
        cursor = next(cursors)
        dt = cursor.fetchall()
        self.df = pd.DataFrame(dt)
        
        if len(self.df) > 0:
            self.df.columns = cursor.column_names
            self.df[DataAccess.COLUMN_ALERT_TIME] = pd.to_datetime(self.df[DataAccess.COLUMN_ALERT_TIME]).apply(utils.inputTimeSanitize)
            self.df[DataAccess.COLUMN_ALERT_METRIC_ACKDT] = pd.to_datetime(self.df[DataAccess.COLUMN_ALERT_METRIC_ACKDT]).apply(utils.inputTimeSanitize)
            self.df[DataAccess.COLUMN_ALERT_METRIC_RECTDT] = pd.to_datetime(self.df[DataAccess.COLUMN_ALERT_METRIC_RECTDT]).apply(utils.inputTimeSanitize)
            self.df[DataAccess.COLUMN_ALERT_METRIC_EXPECTED_RECT] = pd.to_datetime(self.df[DataAccess.COLUMN_ALERT_METRIC_EXPECTED_RECT]).apply(utils.inputTimeSanitize)
            
            if fromtime is not None and totime is not None:
                #Mask out alerts outside range
                
                timeframe_mask = (self.df[self.COLUMN_ALERT_TIME] >= fromtime) & (self.df[self.COLUMN_ALERT_TIME] <= totime)
                self.df = self.df.loc[timeframe_mask]
            
            if len(location_list) > 0:
                #Filter by location
                self.df = self.df[self.df[self.COLUMN_ALERT_LOCATION_ID].isin(location_list)]
                
                #Filter out non-action alerts
                #self.df = self.df[self.df[self.COLUMN_ALERT_CATEGORY] == 'ACTION']
    
    def loadPowerMetric(self, user_id):
        """
        Loads data from all Energy meters
        """
        
        cursors = db_queries.powerview_get_summary(user_id)
        cursor = next(cursors)
        dt = cursor.fetchall()
        self.df = pd.DataFrame(dt)
        if len(self.df) > 0: self.df.columns = cursor.column_names
