
from ..common.dataaccess import DataAccess
from . import user_resources
import json
import os
import traceback

#========Public API========

def getPowerViewSummaryJSON(id_user):
    dal = DataAccess()
    dal.loadPowerMetric(id_user)
    pv = user_resources.PowerView(dal)
    pv.parseData()
    return pv.getJSON()