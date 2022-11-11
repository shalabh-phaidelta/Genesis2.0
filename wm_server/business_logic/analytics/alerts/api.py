
# VERSION 1.0

from flask import request

from ..common import config
from ..common import utils
from ..common.dataaccess import DataAccess
from . import user_resources

def parseAlertInputParameters():
    if request.method == 'POST':
        in_json = request.get_json(force=True)
        start_time = in_json.get('start_time')
        end_time = in_json.get('end_time')
        loc_ids = in_json.get('loc_ids')
    else:
        in_args = request.args
        start_time = in_args.get('start_time')
        end_time = in_args.get('end_time')
        loc_ids = in_args.getlist('loc_ids')
    
    
    #== Default values ==
    #Start time
    if start_time is None:
        start_time = config.getDefaultStartTime()
    
    #End time
    if end_time is None:
        end_time = config.getDefaultEndTime()
        
    #Locations
    if loc_ids is None or len(loc_ids) == 0:
        loc_ids = config.getDefaultLocations()
    
    start_time = utils.inputTimeSanitize(start_time)
    end_time = utils.inputTimeSanitize(end_time)
    
    locations_sanitized = [int(x) for x in loc_ids if x != None]
        
    return start_time, end_time, locations_sanitized


def getAlertsGridLocations(id_user):
    start_time, end_time, locations = parseAlertInputParameters()
    
    dal = DataAccess()
    dal.loadAlertsData(id_user, start_time, end_time, locations)
    
    #Create alert class instance
    al = user_resources.Alerts(dal)
    #Manipulate data to get alert table
    table = al.getAlertTable()
    
    #Structure
    details_table = {
        "alert_heading": config.ALERTS_COLUMN_MAPPING,
        "alert_data": table
    }
    
    return details_table


def getAlertSummaryLocations(id_user):
    start_time, end_time, locations = parseAlertInputParameters()
    
    dal = DataAccess()
    dal.loadAlertsData(id_user, start_time, end_time, locations)
    
    al = user_resources.Alerts(dal)
    summary = al.alertSummaryFiltered()
    
    return summary