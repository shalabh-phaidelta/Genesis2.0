
import pandas as pd

from ..common import config
from ..common.dataaccess import DataAccess

class PowerView:
    dal = None
    JsonData = {}
    
    def __init__(self, dal):
        """
        Sets up data access link
        """
        self.dal = dal
    
    def parseData(self):
        """
        Parses Energy meter summary and
        produces an even simplified summary
        AKA the actual meat of the operation.
        Call this after loadData()
        """
        self.JsonData = {}
        
        overall = {}
        location_summary_list = []
        PowerView.setupNodeProperties(overall)
        
        if len(self.dal.df) > 0:
            self.dal.df[self.dal.COLUMN_POWER_METRIC_VALUE] = self.dal.df[self.dal.COLUMN_POWER_METRIC_VALUE].astype(float)
            location_list = self.dal.df.groupby(self.dal.df[self.dal.COLUMN_POWER_LOCATION_ID])
            
            for loc_id, group_locations in location_list:
                location = {}
                PowerView.setupNodeProperties(location)
                
                mtr_loc_name = group_locations[self.dal.COLUMN_POWER_LOCATION_NAME].mode()
                mtr_loc_alias = group_locations[self.dal.COLUMN_POWER_LOCATION_ALIAS].mode()
                
                location_name, location_alias = "", ""
                if len(mtr_loc_name) > 0: location_name = mtr_loc_name[0]
                if len(mtr_loc_alias) > 0: location_alias = mtr_loc_alias[0]
                
                location["name"] = location_alias
                if pd.isnull(location_alias) or location_alias == "" or location_alias is None:
                    location["name"] = location_name
                
                location['id'] = loc_id
                
                #If the unit is warehouse level, 1, else 0
                meter_kinds = group_locations.groupby(self.dal.COLUMN_POWER_UNIT_IS_WAREHOUSE)
                
                try:
                    mtr_warlvl = meter_kinds.get_group(1)
                    mtr_sous = meter_kinds.get_group(0)
                    
                    #Warehouse-level meter
                    warlvl_measure_type = mtr_warlvl.groupby(self.dal.COLUMN_POWER_METRIC_SUBTYPE)
                    
                    #Unit meters (only the sums)
                    sous_measure_type = mtr_sous.groupby(self.dal.COLUMN_POWER_METRIC_SUBTYPE)
                    
                    df_main_mtd = warlvl_measure_type.get_group(config.EM_TYPE_EM_MAIN_MTD).mode()
                    df_main_24h = warlvl_measure_type.get_group(config.EM_TYPE_EM_MAIN_24H).mode()
                    df_main_1h = warlvl_measure_type.get_group(config.EM_TYPE_EM_MAIN_1H).mode()
                    
                    df_sous_mtd = sous_measure_type.get_group(config.EM_TYPE_EM_SOU_MTD)
                    
                    #Compare MTD values
                    if len(df_main_mtd) > 0:
                        loc_mtd_val = df_main_mtd[self.dal.COLUMN_POWER_METRIC_VALUE][0]
                        location['power_mtd'] = loc_mtd_val
                        
                        if len(df_sous_mtd) > 0:
                            units_sum_mtd_val = df_sous_mtd[self.dal.COLUMN_POWER_METRIC_VALUE].sum()
                            location['power_deviation'] = loc_mtd_val - units_sum_mtd_val
                            location['deviationState'] = config.STATE_NORMAL if location['power_deviation'] < 0 else config.STATE_OOR
                    
                    if len(df_main_24h) > 0:
                        location['power_24h'] = df_main_24h[self.dal.COLUMN_POWER_METRIC_VALUE][0]
                        location['dayState'] = df_main_24h[self.dal.COLUMN_POWER_METRIC_STATE][0]
                    
                    if len(df_main_1h) > 0:
                        location['power_1h'] = df_main_1h[self.dal.COLUMN_POWER_METRIC_VALUE][0]
                        location['hourState'] = df_main_1h[self.dal.COLUMN_POWER_METRIC_STATE][0]                
                    
                    #Add this location to the hierarchy
                    location_summary_list.append(location)
                except KeyError:
                    print("[WARNING] Location '%s' does not have Energy Meters! MTD readings may be off." % location['name'])
        
        overall['locations'] = location_summary_list
        PowerView.doDaSummaryAggregateForParentThing(overall, location_summary_list)
        
        #Update the tree (JSON structure)
        self.JsonData['overall'] = PowerView.finalParseJSON(overall)
    
    
    def getJSON(self):
        """
        Getter function to return JSON data
        """
        return self.JsonData
    
    
    #=== Static functions ===
    def setupNodeProperties(node):
        """
        Static function
        Helper function to populate fields of a node
        """
        node['power_1h'] = 0
        node['power_24h'] = 0
        node['power_mtd'] = 0
        node['power_deviation'] = 0
        
        #States: NORMAL, OUT_OF_RANGE
        node['deviationState'] = config.STATE_NORMAL
        node['dayState'] = config.STATE_NORMAL
        node['hourState'] = config.STATE_NORMAL
    
    def finalParseJSON(json):
        json['power_deviation'] = round(abs(json['power_deviation']), 1)
        for loc in json['locations']:
            loc['power_deviation'] = round(abs(loc['power_deviation']), 1)
        
        
        return json
    
    def doDaSummaryAggregateForParentThing(parentNode, ListOfSummaries):
        """
        Static function
        Sums up a list of summaries and stores in another (parent) node
        """
        
        worst_case_state_day = config.STATE_NORMAL
        worst_case_state_hour = config.STATE_NORMAL
        
        for summary in ListOfSummaries:
            parentNode['power_1h'] += summary['power_1h']
            parentNode['power_24h'] += summary['power_24h']
            parentNode['power_mtd'] += summary['power_mtd']
            parentNode['power_deviation'] += summary['power_deviation']
            
            if summary['dayState'] != config.STATE_NORMAL: worst_case_state_day = summary['dayState']
            if summary['hourState'] != config.STATE_NORMAL: worst_case_state_hour = summary['hourState']
        
        parentNode['deviationState'] = config.STATE_NORMAL if parentNode['power_deviation'] < 0 else config.STATE_OOR
        parentNode['dayState'] = worst_case_state_day
        parentNode['hourState'] = worst_case_state_hour