from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Numeric, ForeignKey
from datetime import datetime

from . models import Base

to_be_created = ['vw_unit_location_mapping_info','vw_metric_summary_latest_unit_level']


view_table = {

    'vw_unit_location_mapping_info': '''SELECT ulum.user_location_unit_id,
    ulum.user_id,
    um2.user_name,
    um.global_unit_name,
    um.unit_alias,
    ulum.location_id,
    lm.global_location_name,
    lm.location_alias,
    lm.latitude,
    lm.longitude,
    ulum.unit_id,
    um.is_warehouse_level_unit ,    
    ulum.is_active,
    ulum.from_date,
    ulum.to_date,
    ulum.created_by,
    ulum.created_ts,
    ulum.updated_by,
    ulum.updated_ts
   FROM user_location_unit_map ulum
     JOIN unit_master um ON ulum.unit_id = um.unit_id
     JOIN location_master lm ON lm.location_id = ulum.location_id
     JOIN user_master um2 ON um2.user_id = ulum.user_id;''',

    'vw_temp_latest_ts': '''SELECT max(mts.sensor_data_calc_ts) AS max_sensor_data_calc_ts,
    mts.metric_name
   FROM mvw_temp_summary mts
  GROUP BY mts.metric_name;''',

    'vw_temp_summary_latest' : """SELECT mts.sensor_data_calc_ts,
    mts.metric_name,
    mts.max_15minutes,
    mts.avg_15_minutes,
    mts.min_15minutes,
    sm.sensor_id,
    ( SELECT usm.unit_id
           FROM unit_sensor_map usm
          WHERE usm.sensor_id = sm.sensor_id) AS unit_id,
    sm.is_active,
    sm.created_by,
    sm.created_ts,
    sm.updated_by,
    sm.updated_ts
   FROM mvw_temp_summary mts
     JOIN sensor_master sm ON sm.global_sensor_name::text = mts.metric_name
     JOIN vw_temp_latest_ts vtlt ON mts.sensor_data_calc_ts = vtlt.max_sensor_data_calc_ts AND mts.metric_name = vtlt.metric_name;""",

    'vw_pres_latest_ts':'''SELECT max(mps.sensor_data_calc_ts) AS max_sensor_data_calc_ts,
    mps.metric_name
   FROM mvw_pres_summary mps
  GROUP BY mps.metric_name;''',

    'vw_pres_summary_latest':'''SELECT mps.sensor_data_calc_ts,
    mps.metric_name,
    mps.max_15minutes,
    mps.avg_15_minutes,
    mps.min_15minutes,
    sm.sensor_id,
    ( SELECT usm.unit_id
           FROM unit_sensor_map usm
          WHERE usm.sensor_id = sm.sensor_id) AS unit_id,
    sm.is_active,
    sm.created_by,
    sm.created_ts,
    sm.updated_by,
    sm.updated_ts
   FROM mvw_pres_summary mps
     JOIN sensor_master sm ON sm.global_sensor_name::text = mps.metric_name
     JOIN vw_pres_latest_ts vplt ON mps.sensor_data_calc_ts = vplt.max_sensor_data_calc_ts AND mps.metric_name = vplt.metric_name;''',

    'vw_metric_summary_latest':'''SELECT vpsl.sensor_data_calc_ts,
        vpsl.metric_name,
        vpsl.max_15minutes,
        vpsl.avg_15_minutes,
        vpsl.min_15minutes,
        vpsl.sensor_id,
        vpsl.unit_id,
        vpsl.is_active,
        vpsl.created_by,
        vpsl.created_ts,
        vpsl.updated_by,
        vpsl.updated_ts
    FROM vw_pres_summary_latest vpsl
    UNION
    SELECT vtsl.sensor_data_calc_ts,
        vtsl.metric_name,
        vtsl.max_15minutes,
        vtsl.avg_15_minutes,
        vtsl.min_15minutes,
        vtsl.sensor_id,
        vtsl.unit_id,
        vtsl.is_active,
        vtsl.created_by,
        vtsl.created_ts,
        vtsl.updated_by,
        vtsl.updated_ts
    FROM vw_temp_summary_latest vtsl;''',

    'vw_metric_summary_latest_unit_level':'''SELECT vmsl.sensor_data_calc_ts,
    sm.sensor_type,
    vulmi.user_id,
    vulmi.location_id,
    vulmi.global_location_name AS location_name,
    vulmi.location_alias,
    vulmi.latitude,
    vulmi.longitude,
    vmsl.unit_id,
    vulmi.global_unit_name AS unit_name,
    vulmi.unit_alias,
    vulmi.is_warehouse_level_unit ,    
    sm.sensor_id,
    sm.global_sensor_name AS sensor_name,
    sm.alias AS sensor_alias,
    vmsl.max_15minutes AS max_value,
    vmsl.avg_15_minutes AS value,
    vmsl.min_15minutes AS min_value,
    sm.metric_unit AS unit,
    sm.aggregation_time_period AS value_duration
   FROM vw_metric_summary_latest vmsl
     JOIN sensor_master sm ON sm.sensor_id = vmsl.sensor_id
     JOIN vw_unit_location_mapping_info vulmi ON vulmi.unit_id = vmsl.unit_id;'''
     
}

materialize_view = {

     'history_summary':'''SELECT time_bucket('00:15:00'::interval, hn."time") AS day,
    hn.history_id,
    max(hn.value) AS high,
    avg(hn.value) AS average,
    min(hn.value) AS low
   FROM history_num hn
  GROUP BY (time_bucket('00:15:00'::interval, hn."time")), hn.history_id;''',

    'mvw_pres_summary':'''SELECT time_bucket('00:15:00'::interval, hn."time") AS sensor_data_calc_ts,
        hn.history_id AS metric_name,
        max(hn.value) AS max_15minutes,
        avg(hn.value) AS avg_15_minutes,
        min(hn.value) AS min_15minutes
    FROM history_num hn
    WHERE hn.history_id ~~ '%_Pres'::text 
    GROUP BY (time_bucket('00:15:00'::interval, hn."time")), hn.history_id;''',
    
    'mvw_temp_summary':'''SELECT time_bucket('00:15:00'::interval, hn."time") AS sensor_data_calc_ts,
        hn.history_id AS metric_name,
        max(hn.value) AS max_15minutes,
        avg(hn.value) AS avg_15_minutes,
        min(hn.value) AS min_15minutes
    FROM history_num hn
    WHERE hn.history_id ~~ '%_Temp'::text 
    GROUP BY (time_bucket('00:15:00'::interval, hn."time")), hn.history_id;''',



}

class VM_Temp_summary_latest(Base):
    __tablename__ = 'vw_temp_summary_latest'
    sensor_data_calc_ts = Column(DateTime, primary_key = True)
    metric_name = Column(String)
    max_15minutes = Column(Numeric)
    avg_15_minutes = Column(Numeric)
    min_15minutes = Column(Numeric)
    sensor_id = Column(Integer)
    unit_id = Column(Integer)
    is_active = Column(Boolean)
    created_by = Column(String)
    created_ts = Column(DateTime)
    updated_by = Column(String)
    updated_ts = Column(DateTime)

    def to_dict(self):
        return {
            "sensor_data_calc_ts": self.sensor_data_calc_ts,
            "metric_name": self.metric_name,
            "max_15minutes": self.max_15minutes,
            "avg_15_minutes": self.avg_15_minutes,
            "min_15minutes": self.min_15minutes,
            "sensor_id": self.sensor_id,
            "unit_id": self.unit_id,
            "is_active": self.is_active,
            "created_by": self.created_by,
            "created_ts": self.created_ts,
            "updated_by": self.updated_by,
            "updated_ts": self.updated_ts
        }

class VM_Metric_summary_latest(Base):
    __tablename__ = 'vw_metric_summary_latest'
    sensor_data_calc_ts = Column(DateTime, primary_key = True)
    metric_name = Column(String)
    max_15minutes = Column(Numeric)
    avg_15_minutes = Column(Numeric)
    min_15minutes = Column(Numeric)
    sensor_id = Column(Integer)
    unit_id = Column(Integer)
    is_active = Column(Boolean)
    created_by = Column(String)
    created_ts = Column(DateTime)
    updated_by = Column(String)
    updated_ts = Column(DateTime)

    def to_dict(self):
        return {
            "sensor_data_calc_ts": self.sensor_data_calc_ts,
            "metric_name": self.metric_name,
            "max_15minutes": self.max_15minutes,
            "avg_15_minutes": self.avg_15_minutes,
            "min_15minutes": self.min_15minutes,
            "sensor_id": self.sensor_id,
            "unit_id": self.unit_id,
            "is_active": self.is_active,
            "created_by": self.created_by,
            "created_ts": self.created_ts,
            "updated_by": self.updated_by,
            "updated_ts": self.updated_ts
        }

class VM_Unit_location_mapping_info(Base):
    __tablename__ = 'vw_unit_location_mapping_info'
    user_id = Column(Integer, primary_key = True)
    user_name = Column(String)
    global_unit_name = Column(String)
    unit_alias = Column(String)
    location_id = Column(Integer)
    global_location_name = Column(String)
    location_alias = Column(String)
    longitude = Column(String)
    latitude = Column(String)
    unit_id = Column(Integer)
    is_active = Column(Boolean)
    from_date = Column(DateTime)
    to_date = Column(DateTime)
    created_by = Column(String)
    created_ts = Column(DateTime)
    updated_by = Column(String)
    updated_ts = Column(DateTime)

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "user_name": self.user_name,
            "global_unit_name": self.global_unit_name,
            "unit_alias": self.unit_alias,
            "location_id": self.location_id,
            "global_location_name": self.global_location_name,
            "location_alias": self.location_alias,
            "latitude" :self.latitude,
            "longitude" : self.longitude,
            "unit_id": self.unit_id,
            "is_active": self.is_active,
            "from_date" : self.from_date,
            "to_date" : self.to_date,
            "created_by": self.created_by,
            "created_ts": self.created_ts,
            "updated_by": self.updated_by,
            "updated_ts": self.updated_ts
        }

class VW_Metric_Summary_Latest_unit_Level(Base):
    __tablename__ = 'vw_metric_summary_latest_unit_level'
    sensor_data_calc_ts = Column(DateTime, primary_key = True)
    sensor_type = Column(String)
    user_id = Column(Integer)
    location_id = Column(Integer)
    location_name = Column(String)
    location_alias = Column(String)
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    unit_id = Column(Integer)
    unit_name = Column(String)
    unit_alias = Column(String)
    is_warehouse_level_unit = Column(Boolean)
    sensor_id = Column(Integer)
    sensor_name = Column(String)
    sensor_alias = Column(String)
    max_value = Column(Numeric)
    value = Column(Numeric)
    min_value = Column(Numeric)
    unit = Column(String)
    value_duration = Column(Integer)



    def to_dict(self):
        return {
            "sensor_data_calc_ts": self.sensor_data_calc_ts,
            "sensor_type": self.sensor_type,
            "user_id" : self.user_id,
            "location_id": self.location_id,
            "location_name": self.location_name,
            "location_alias": self.location_alias,
            "latitude": self.latitude,
            "longitude": self.longitude,            
            "unit_id": self.unit_id,
            "unit_name": self.unit_name,
            "unit_alias": self.unit_alias,
            "is_warehouse_level_unit" : self.is_warehouse_level_unit,
            "sensor_id": self.sensor_id,
            "sensor_name": self.sensor_name,
            "sensor_alias": self.sensor_alias,
            "max_value": self.max_value,
            "value": self.value,
            "min_value": self.min_value,
            "unit": self.unit,
            "value_duration": self.value_duration
            
            }

