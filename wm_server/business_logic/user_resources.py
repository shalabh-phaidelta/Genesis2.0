from decimal import Decimal
import json

from ..data_access import db_queries , data_frame
#from .analytics.charts import api as chart_api

# from importlib.resources import Resource

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # 👇️ if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, Decimal):
            return str(obj)
        # 👇️ otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)

class UserLocation():
    def get(self, user_id):
        location = db_queries.all_locations(user_id)
        return location


class Location_Summary():
    def get(self, location_id, user_id):
        metric_data = []
        result =  db_queries.data_for_metrics(user_id, location_id)
        for i in result: 
            metric_data.append({
                    'user_id'          : i.user_id,
                    'user_name'    : i.user_name,
                    'role' : 'Role', #when roles are added
                    'role_id' : 'Role_ID', # when roles are added
                    'metrics_out_count' : 0, # will need to calculate :'sum([x.metrics_out_count for x in tmp if x.state == nrm])'
                    'state' : 'Normal', #will need to calculate
                    'location_id'  : location_id,
                    'location_name' : i.global_location_name
            })
        ret = {}
        ret['metrics'] = metric_data
        return ret



class Metrics_Warehouse():
    def get(self, location_id :int, user_id : int):
        # user_id = 3

        return {
            'wv_warehouse_metrics': self.warehouse_view_warehouse_level_metrics(user_id, location_id),
            'wv_unit_summary': self.warehouse_view_unit_level_summary(user_id, location_id),
        }
        
    @staticmethod
    def warehouse_view_warehouse_level_metrics( user_id : int, location_id : int):
        response = []
        data = db_queries.warehouse_view_warehouse_level_metrics(user_id, location_id)
        for i in data:
            response.append({
                'Block': 'Warehouse Level Metric',
                'Location Id': location_id,
                'Metric Type': i.sensor_type,
                'Sensor Id': i.sensor_id,
                'Sensor Name': i.sensor_name,
                'Value': i.value,
                'Unit': i.unit_id,
                'Percentage': 100,
                'State': 'Normal', #need to Calculate
                'Value Duration Minutes': i.value_duration,
                'Threshold crosses': 0 # need to insert threshold checking
            })
        return response

    @staticmethod
    def warehouse_view_unit_level_summary(user_id : int , location_id : int):
        response = []
        data = db_queries.warehouse_view_unit_summary(user_id, location_id)
        for i in data:
            response.append({
                'Block'          : 'Unit Summary',
                'Location Id'    : location_id,
                'Location Name'  : i.location_name,
                'Location Alias' : i.location_alias,
                'Unit Id'        : i.unit_id,
                'Unit Name'      : i.unit_name,
                'Unit Alias'     : i.unit_alias,
                'Value'          : i.value,
                'State'          : 'Normal', #will need to calculate
            })
        return response

class MetricsChartHistory():
    def get(self, metric_id, user_id, parameter):
      try:
        metric = MetricHistoryJSON(user_id=user_id, metric_id=metric_id)
        data  = metric.get_Metric_Chart_History_JSON(parameter)
      except IndexError :
        data = {}
      return data


class MetricHistoryJSON():
  def __init__(self,  metric_id, user_id):
    self.metric_id = metric_id
    self.metric_data = db_queries.get_latest_metric_data(metric_id, user_id)[0]
    self.user_id = user_id
    self.sensor_type = self.metric_data['sensor_type']

  def get_MetricHistory(self, parameter):
    df = data_frame.get_data_from_metric(self.metric_id, self.sensor_type)
    df =df[(df['sensor_data_calc_ts']> parameter.start_time) & (df['sensor_data_calc_ts']< parameter.end_time)]
    response = []
    print(df)
    try:
        for line_item in df.iloc:
            print(line_item)
            response.append({
                              "x": line_item['sensor_data_calc_ts'],
                              "y": line_item['avg_15_minutes'],
                              "state": "NORMAL",
                              "minmax": {
                                "min": line_item['min_15minutes'],
                                "max": line_item['max_15minutes']
                              },
                              "subTime": {
                                "min": line_item['sensor_data_calc_ts'],
                                "max": line_item['sensor_data_calc_ts']
                              }
                            }
                            )
        return response
    except NotImplementedError:
        return []

  def get_Metric_Chart_History_JSON(self, parameter):
    dataset  = self.get_MetricHistory(parameter)
    data = {
          'from':parameter.start_time,
          'to':parameter.end_time,
          'interval' : parameter.interval,
            "intervals": [
                            "15M",
                            "30M",
                            "1H",
                            "1D"
                          ],
          'metric_id' : self.metric_data['metric_id'],
          'metric_name' : self.metric_data['metric_name'],
          "location_id": self.metric_data['location_id'],
          "location_name": self.metric_data['location_name'],
          "unit_id": self.metric_data['unit_id'],
          "unit_name": self.metric_data['unit_name'],
          "is_warehouse_level_unit": self.metric_data['is_warehouse_level_unit'],
          "threshold": {
            "upper": None,
            "lower": None
          },
          "control_limits": {
            "LCL": 'Lower limit',
            "UCL": 'Upper limit'
          },
          "mean": 'mean need to be calculated',
          "stdev": 'STD need to be calculated',
          "measure_unit": [
            self.metric_data['measure_unit'],
            self.metric_data['sensor_type']
          ],
          "label": self.metric_data['label'],
          "dataset": dataset
      }
    return data



class Metrics_Unit():
    def get(self, location_id, unit_id, user_id):
              return {
            'uv_unit_metrics': self.unit_view_unit_metrics(user_id, location_id, unit_id),
        }
    @staticmethod
    def unit_view_unit_metrics(user_id : int, location_id : int, unit_id : int):
        return db_queries.unit_view_unit_metrics(user_id, location_id, unit_id)


class User():
    def user_name_list(self):
        user_list = db_queries.get_user_list()
        return user_list





