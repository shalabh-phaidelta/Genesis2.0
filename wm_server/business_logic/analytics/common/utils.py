
from pandas import to_datetime
import os
from datetime import datetime, timedelta

def metric_id_to_dict(metric):
    metr = metric.split("_")
    metric_location = {
        "Region": metr[0],
        "Warehouse": metr[1],
        "Building": metr[2],
        "Floor": metr[3],
        "Unit": metr[4]
    }
    
    if len(metr)>5:
        metric_location["metric_id"] = metr[5:]
    
    return metric_location

def td_sec(sec):
    return timedelta(seconds = sec)

def td_get_total_seconds(td):
    return td.seconds + td.days * 86400

def excel_date(date1):
    temp = datetime(1899, 12, 30)
    delta = date1 - temp
    return float(delta.days) + (float(delta.seconds) / 86400)

def getCurrentTime(tz="Asia/Calcutta"):
    return to_datetime(datetime.now()).tz_localize(tz)

def getReportFileName(dal, ext):
    cTime = getCurrentTime().strftime("%Y%m%d_%H%M%S")
    rptFile = "Metric report - %s_%s.%s" % (cTime, dal.metadata["metric_name"], ext)
    return rptFile

def exportReport(inputIO, metric, ext):
    if inputIO is None:
        return "Failed"
    
    cTime = getCurrentTime().strftime("%Y%m%d_%H%M%S")
    rptFile = "Metric report - %s_%s.%s" % (cTime, metric, ext)
    relative_path = os.path.join("reports", rptFile)
    url_path = "/reports/" + rptFile
    
    with open(relative_path, 'wb') as out:
        out.write(inputIO.read())
    
    return url_path

def dt_local(t):
    return to_datetime(t).tz_convert('Asia/Calcutta')

#Convert common time intervals to human-readable version
def td2human(td):
    if td == timedelta(minutes=15):
        return "15 minutes"
    elif td == timedelta(minutes=30):
        return "30 minutes"
    elif td == timedelta(hours=1):
        return "1 hour"
    elif td == timedelta(days=1):
        return "Daily"
    
    return str(td)

#Simple bound check function
def isWithinBoundary(val, ll, ul):
    if ll is None and ul is None:
        return True
    
    if (ul is None and ll is not None):
        return val >= ll
        
    if (ul is not None and ll is None):
        return val <= ul
    
    return val >= ll and val <= ul

def dtToDB(t):
    print(t)
    return t.strftime('%Y-%m-%d %H:%M:%S')

#Excel hax
def makeFormulaOfValuesRepeated(value, length):
    formula = "={"
    formula += ",".join([value for i in range(length)])
    formula += "}"
    return formula

#https://stackoverflow.com/a/56245404
def inputTimeSanitize(time):
    return to_datetime(time, utc=True).tz_convert('UTC')