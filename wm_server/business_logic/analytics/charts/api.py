"""
API Functions that call appropriate charting functions
"""


from ..common import config
from ..common import utils
from . import user_resources

def parseMetricInputParameters():
    if request.method == 'POST':
        in_json = request.get_json(force=True)
        start_time = in_json.get('start_time')
        end_time = in_json.get('end_time')
        interval = in_json.get('interval')
    else:
        in_args = request.args
        start_time = in_args.get('start_time')
        end_time = in_args.get('end_time')
        interval = in_args.get('interval')
    
    #== Default values ==
    
    #Start time
    if start_time is None:
        start_time = config.getDefaultStartTime()
    
    #End time
    if end_time is None:
        end_time = config.getDefaultEndTime()
    
    #Interval
    if interval is None:
        interval = config.getDefaultInterval()
    
    #Convert to Pandas Timeframe
    start_time = utils.inputTimeSanitize(start_time)
    end_time = utils.inputTimeSanitize(end_time)
    
    if interval in config.METRIC_INTERVAL_AGGREGATE:
        interval = config.METRIC_INTERVAL_AGGREGATE[interval]
    else:
        raise ValueError("Invalid aggregation interval: %s " % interval)

    return start_time, end_time, interval

def getMetricChartHistoryJSON(id_user, metric_id, parameter):
    # start_time, end_time, interval = parseMetricInputParameters()
    outputJSON = user_resources.chartJSON(id_user, metric_id, parameter['start_time'], parameter['end_time'], parameter['inter_val'])
    print(outputJSON)
    return outputJSON

def getMetricChartHistoryXLSX(id_user, metric_id):
    start_time, end_time, interval = parseMetricInputParameters()
    rptData = user_resources.chartXLSX(id_user, metric_id, start_time, end_time, interval)
    return utils.getReportFileName(rptData[0], "xlsx"), rptData[1]

def getMetricChartHistoryPDF(id_user, metric_id):
    start_time, end_time, interval = parseMetricInputParameters()
    rptData = user_resources.chartPDF(id_user, metric_id, start_time, end_time, interval)
    return utils.getReportFileName(rptData[0], "pdf"), rptData[1]
