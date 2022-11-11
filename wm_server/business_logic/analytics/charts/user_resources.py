
from ..common import config
from ..common.dataaccess import DataAccess

from . import timeseries
from . import timeline

def getMetricObject(dal):
    """
    Metric Factory to create a metric instance from type
    """
    type = dal.metadata['metric_type']
    subtype = dal.metadata['metric_subtype']
    
    #Timeseries
    if type == config.METRIC_TYPE_TEMP:
        return timeseries.Temperature(dal)
    elif type == config.METRIC_TYPE_EM_KWH:
        return timeseries.EnergyMeter(dal)
    elif type == config.METRIC_TYPE_EM_PF:
        return timeseries.EnergyMeter_PF(dal)
    elif type == config.METRIC_TYPE_HUMIDITY:
        return timeseries.Humidity(dal)
    elif type == config.METRIC_TYPE_DG:
        if subtype == config.METRIC_SUBTYPE_DG_RH:
            return timeseries.DGRunHours(dal)
    elif type == config.METRIC_TYPE_SOLAR:
        if subtype == config.METRIC_SUBTYPE_SOLAR_ET:
            return timeseries.SolarEMTotal(dal)
    #Timeline
    elif type == config.METRIC_TYPE_DOOR:
        return timeline.Door(dal)
    elif type == config.METRIC_TYPE_WATER_TANK:
        return timeline.WaterTank(dal)
    elif type == config.METRIC_TYPE_PANIC:
        return timeline.Panic(dal)
    elif type == config.METRIC_TYPE_VESDA:
        return timeline.VESDA(dal)
    #Not enough data
    elif type == None:
        return None
    else:
        print("Not implemented: metric type - %s." % type)
        return None


#need to update method

def chartJSON(id_user, metric_id, start_time, end_time, interval):
    dal = DataAccess()
    dal.loadMetricData(id_user, metric_id, start_time, end_time, interval)
    metric_obj = getMetricObject(dal)
    if metric_obj is None: return {}
    output = metric_obj.aggregate(interval)
    print('output chart JSON', output)
    return output

def chartXLSX(id_user, metric_id, start_time, end_time, interval):
    dal = DataAccess()
    dal.loadMetricData(id_user, metric_id, start_time, end_time, interval)
    metric_obj = getMetricObject(dal)
    if metric_obj is None: return dal, None
    output = metric_obj.generateXLSX(interval)
    return dal, output

def chartPDF(id_user, metric_id, start_time, end_time, interval):
    dal = DataAccess()
    dal.loadMetricData(id_user, metric_id, start_time, end_time, interval)
    metric_obj = getMetricObject(dal)
    if metric_obj is None: return dal, None
    output = metric_obj.generatePDF(interval)
    return dal, output