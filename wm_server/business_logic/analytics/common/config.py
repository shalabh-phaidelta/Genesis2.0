from datetime import datetime, timedelta, timezone
from datetime import date

#Whether the number of days since rectification counts
#from acknowledgment date instead of alert date
USE_ACK_AS_DAYS_FROM_RECT = True

#If set to true, excel reports will be READONLY.
EXCEL_WRITE_PROTECT = False

#Seconds in a day
SECONDS_IN_DAY = 86400

#To how many decimal places the data points and
#characteristic data should be rounded to
ROUND_DECIMAL_PTS = 1

##Strings##

#Metric Types
METRIC_TYPE_TEMP        = "Temperature"
METRIC_TYPE_HUMIDITY    = "RH"
METRIC_TYPE_EM_KWH      = "Energy Meter- units"
METRIC_TYPE_EM_PF       = "Energy Meter- Power factor"
METRIC_TYPE_DOOR        = "Doors"
METRIC_TYPE_WATER_TANK  = "Water tank"
METRIC_TYPE_PANIC       = "Emergency"
METRIC_TYPE_DG          = "Power - DG"
METRIC_SUBTYPE_DG_RH    = "DG Run Hours"
METRIC_TYPE_SOLAR       = "Solar"
METRIC_SUBTYPE_SOLAR_ET = "Solar Energy Total"
METRIC_TYPE_VESDA       = "Fire"
METRIC_SUBTYPE_VESDA_Action           = "VESDA Action"
METRIC_SUBTYPE_VESDA_Fire1            = "VESDA Fire1"
METRIC_SUBTYPE_VESDA_Fire2            = "VESDA Fire2"
METRIC_SUBTYPE_VESDA_FlowFaultLow     = "VESDA FlowFaultLow"
METRIC_SUBTYPE_VESDA_FlowFaultHigh    = "VESDA FlowFaultHigh"
METRIC_SUBTYPE_VESDA_FilterClogging   = "VESDA FilterClogging"
METRIC_SUBTYPE_VESDA_NetworkFault     = "VESDA NetworkFault"
METRIC_SUBTYPE_VESDA_AspiratorSpeed   = "VESDA AspiratorSpeed"

#Timeline metric Logic type
LOGIC_ACTIVE_HIGH = "active_high"
LOGIC_ACTIVE_LOW = "active_low"

#Human readable Time stamp format
HUMAN_TIME_FORMAT = '%d %b %y %H:%M:%S'
STANDARD_TIME_FORMAT = '%y %b %d %H:%M'
HUMAN_EXCEL_DATE_FORMAT = 'dd-mm-yyyy HH:MM:SS'

#Point value states
STATE_NORMAL   = "NORMAL"
STATE_OOR      = "OUT_OF_RANGE"
STATE_INACTIVE = "INACTIVE"


#String constants
STRING_NA = "N/A"

#Chart config
CHART_SCALE = (3, 1.7)
CHART_TICKS_TIME_FORMAT = "dd/mm/yyyy HH:MM"
CHART_LINE_COLOR = "black"
CHART_LINE_THICKNESS = 1
CHART_MARKER_KIND = "circle"
CHART_MARKER_RADIUS = 6
CHART_MARKER_COLOR_NORMAL = "green"
CHART_MARKER_COLOR_OOR = "red"
CHART_CONTROLLIMIT_COLOR = "blue"
CHART_CONTROLLIMIT_THICKNESS = 1
CHART_CONTROLLIMIT_DASHTYPE = ("dash_dot", "dashdot")
CHART_THRESHOLD_COLOR = "red"
CHART_THRESHOLD_THICKNESS = 1
CHART_PLOTLY_WIDTH = 1280
CHART_PLOTLY_HEIGHT = 960
CHART_PDF_WIDTH_CM = 20
CHART_TRENDLINE_COLOR = 'black'
CHART_TRENDLINE_THICKNESS = 1
CHART_TRENDLINE_TYPE = ('long_dash', 'dashdot')
CHART_XAXIS_FORMAT_PLOTLY = "%Y-%b-%d %H:%M"

#Report / Chart Title string formats
REPORT_TITLE_GENERAL = "Report for {s_name} from {t_start} to {t_end}"
CHART_TITLE_GENERAL = "{s_name} {t_interval} average"
CHART_TITLE_TIMELINE = "{s_name}"
CHART_TITLE_ENERGY_CONSUMPTION = "{s_name} {t_interval} Consumption"
CHART_TITLE_ENERGY_CUMULATIVE = "{s_name} Cumulative Consumption"
CHART_TRENDLINE_SERIES_NAME = "Trend"

#Powerview config
EM_TYPE_EM_MAIN_MTD = 'EM Main - MTD'
EM_TYPE_EM_MAIN_24H = 'EM Main - Last 24 hours'
EM_TYPE_EM_MAIN_1H  = 'EM Main - Last 1 hour'
EM_TYPE_EM_SOU_MTD  = 'EM - MTD'

#Default parameters
def getDefaultStartTime():
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

def getDefaultEndTime():
    return datetime.now(timezone.utc)

def getDefaultLocations():
    return list(DATA_LOCATIONS.keys())

def getDefaultInterval():
    return "15M"

#===Mapping tables===


#Metric Metadata

METRIC_LOCATION_FULLNAME = {
    "ver": "Verna",
    "kud": "Kudal",
    
    
    "inv": "Invalid"
}

#Which intervals are applicable for the unit
METRIC_INTERVALS = {
    METRIC_TYPE_DOOR: None,
    METRIC_TYPE_WATER_TANK: None,
    METRIC_TYPE_PANIC: None,
    METRIC_TYPE_TEMP: ["15M", "30M", "1H", "1D"],
    METRIC_TYPE_HUMIDITY: ["15M", "30M", "1H", "1D"],
    METRIC_TYPE_EM_KWH: ["1H", "1D"],
    METRIC_TYPE_EM_PF: ["1H", "1D"],
    METRIC_TYPE_DG: ["1H", "1D"],
    "mp": None,
    METRIC_TYPE_VESDA: None
}

#Interval to timedelta lookup table
METRIC_INTERVAL_AGGREGATE = {
    "15M": timedelta(minutes=15),
    "30M": timedelta(minutes=30),
    "1H": timedelta(hours=1),
    "1D": timedelta(days=1)
}

#Unit/legend for each type of metric
METRIC_UNIT = {
    METRIC_TYPE_DOOR: None,
    METRIC_TYPE_WATER_TANK: None,
    METRIC_TYPE_PANIC: None,
    METRIC_TYPE_HUMIDITY: ("%", "Humidity"),
    METRIC_TYPE_TEMP: ("°C", "Temperature"),
    METRIC_TYPE_EM_KWH: ("KWh", "Energy"),
    METRIC_TYPE_EM_PF: ("", "Power Factor"),
    METRIC_TYPE_DG: (None, "Hours"),
    METRIC_TYPE_SOLAR: ("KWh", "Energy"),
    "mp": ("", "Staff count"),
    METRIC_TYPE_VESDA: None
}

#Temporary
DATA_LOCATIONS = {
    1: "VER"
}


#Column map for alerts
ALERTS_COLUMN_MAPPING = {
    "alert_status": "Alert Status",
    "alert_category": "Alert Category",
    "location_name": "Location",
    "unit_name": "Unit",
    "alert_type": "Alert Type",
    "client": "Client",
    "metric_name": "Metric Name",
    "target": "Target Value",
    "actual": "Actual Value",
    "percent_dev": "% Deviation",
    "alert_date": "Alert Datetime",
    "ack_date": "Acknowledged Datetime",
    "ack_by_name": "Acknowledged By",
    "rect_action": "Rectification Action",
    "expected_rect_time": "Expected Rectification Date",
    "rect_by_name": "Rectified By",
    "rect_date": "Rectification Date",
    "ack_days": "Days To Acknowledge",
    "rect_days": "Days To Rectify",
    "alert_eta_change_reason": "ETA Change Reason",
    "alert_rect_action_change_reason": "Rectification Action Change Reason",
}

ALERT_STATUS_ACK = "ACK"
ALERT_STATUS_RECT = "RECT"
ALERT_STATUS_OPEN = "OPEN"

ALERT_SUMMARY_CURRENT = "current_time"
ALERT_SUMMARY_LAST1H = "alerts_last_1h"
ALERT_SUMMARY_FROM0H = "alerts_from_0h"
ALERT_SUMMARY_OPEN0H = "alerts_open_before_0h"
ALERT_SUMMARY_AK2DAY = "alerts_ack_today"
ALERT_SUMMARY_RT2DAY = "alerts_rect_today"
ALERT_SUMMARY_OP2DAY = "alerts_open_today"
ALERT_SUMMARY_MTD_ALERTS = "interval_alerts"
ALERT_SUMMARY_MTD_ACKS = "interval_ack"
ALERT_SUMMARY_MTD_NACKS = "interval_nack"
ALERT_SUMMARY_MTD_RECTS = "interval_rect"
ALERT_SUMMARY_MTD_NRECTS = "interval_nrect"
ALERT_SUMMARY_AGING_ACK = "interval_ack_aging"
ALERT_SUMMARY_AGING_NACK = "interval_nack_aging"
ALERT_SUMMARY_AGING_RECT = "interval_rect_aging"
ALERT_SUMMARY_AGING_NRECT = "interval_nrect_aging"

ALERT_SUMMARY_DEFAULT = [
    ALERT_SUMMARY_CURRENT,
    ALERT_SUMMARY_LAST1H,
    ALERT_SUMMARY_FROM0H,
    ALERT_SUMMARY_OPEN0H,
    ALERT_SUMMARY_AK2DAY,
    ALERT_SUMMARY_RT2DAY,
    ALERT_SUMMARY_OP2DAY,
    ALERT_SUMMARY_MTD_ALERTS,
    ALERT_SUMMARY_MTD_ACKS,
    ALERT_SUMMARY_MTD_NACKS,
    ALERT_SUMMARY_MTD_RECTS,
    ALERT_SUMMARY_MTD_NRECTS,
    ALERT_SUMMARY_AGING_ACK,
    ALERT_SUMMARY_AGING_NACK,
    ALERT_SUMMARY_AGING_RECT,
    ALERT_SUMMARY_AGING_NRECT
]
