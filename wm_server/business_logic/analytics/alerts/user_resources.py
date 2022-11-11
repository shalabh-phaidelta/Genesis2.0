
import pandas as pd
from datetime import timedelta, timezone

from ..common import config
from ..common import utils

from ..common.dataaccess import DataAccess

class Alerts:
    dal = None
    
    def __init__(self, dal):
        self.dal = dal
    
    def getAlertTable(self):
        """
        Alert details table
        """
        
        alert_table = []
        for i, row in self.dal.df.iterrows():
            alert_id = row[DataAccess.COLUMN_ALERT_ID]
            alert_datetime = row[DataAccess.COLUMN_ALERT_TIME]
            
            #These will be "null" by default
            days_to_ack = None
            days_to_rect = None
            expected_rect = None
            rect_action = None
            rect_action_id = None
            ack_by_id = None
            rect_by_id = None
            ack_by_name = None
            rect_by_name = None
            ack_datetime_str = None
            rect_datetime_str = None
            
            #Will be 'N/A' by default
            alert_category = config.STRING_NA
            alert_eta_change_reason = config.STRING_NA
            alert_rect_action_change_reason = config.STRING_NA
            alert_client = config.STRING_NA     #TODO
            metric_value_target = config.STRING_NA
            metric_value_actual = config.STRING_NA
            P_dev = config.STRING_NA
            
            #==Alert time==
            alert_datetime_str = alert_datetime.isoformat()
            alert_status = row[DataAccess.COLUMN_ALERT_METRIC_STATUS]
            
            rect_action = row[DataAccess.COLUMN_ALERT_METRIC_RECTHOW]
            if not pd.isnull(row[DataAccess.COLUMN_ALERT_METRIC_RECTHOW_ID]):
                rect_action_id = int(row[DataAccess.COLUMN_ALERT_METRIC_RECTHOW_ID])
            
            #Ack Date
            if not pd.isnull(row[DataAccess.COLUMN_ALERT_METRIC_ACKDT]):
                ack_datetime = row[DataAccess.COLUMN_ALERT_METRIC_ACKDT]
                ack_datetime_str = ack_datetime.isoformat()
                
                if not pd.isnull(row[DataAccess.COLUMN_ALERT_METRIC_ACKBY]):
                    ack_by_id = int(row[DataAccess.COLUMN_ALERT_METRIC_ACKBY])
                    ack_by_name = row[DataAccess.COLUMN_ALERT_METRIC_ACKBY_NAME]
                
                expected_rect = row[DataAccess.COLUMN_ALERT_METRIC_EXPECTED_RECT]
                if not pd.isnull(row[DataAccess.COLUMN_ALERT_METRIC_EXPECTED_RECT]):
                    expected_rect = expected_rect.isoformat()
                else:
                    expected_rect = None
                
                days_to_ack = (ack_datetime - alert_datetime).days
            
            #Rect date
            if not pd.isnull(row[DataAccess.COLUMN_ALERT_METRIC_RECTDT]):
                rect_datetime = row[DataAccess.COLUMN_ALERT_METRIC_RECTDT]
                rect_datetime_str = rect_datetime.isoformat()
                
                if not pd.isnull(row[DataAccess.COLUMN_ALERT_METRIC_RECTBY]):
                    rect_by_id = int(row[DataAccess.COLUMN_ALERT_METRIC_RECTBY])
                    rect_by_name = row[DataAccess.COLUMN_ALERT_METRIC_RECTBY_NAME]
                
                if not pd.isnull(row[DataAccess.COLUMN_ALERT_METRIC_ACKDT]) and config.USE_ACK_AS_DAYS_FROM_RECT:
                    ack_datetime = row[DataAccess.COLUMN_ALERT_METRIC_ACKDT]
                    time_rect = rect_datetime - ack_datetime
                else:
                    time_rect = rect_datetime - alert_datetime
                days_to_rect = time_rect.days
            
            
            #Change reasons
            if not pd.isnull(row[DataAccess.COLUMN_ALERT_ETA_CHANGE_REASON]):
                alert_eta_change_reason = row[DataAccess.COLUMN_ALERT_ETA_CHANGE_REASON]
            if not pd.isnull(row[DataAccess.COLUMN_ALERT_RECT_ACTION_CHANGE_REASON]):
                alert_rect_action_change_reason = row[DataAccess.COLUMN_ALERT_RECT_ACTION_CHANGE_REASON]
            
            #==Metric information==
            alert_location_name = row[DataAccess.COLUMN_ALERT_LOCATION_ALIAS]
            alert_location_id = int(row[DataAccess.COLUMN_ALERT_LOCATION_ID])
            alert_unit_name = row[DataAccess.COLUMN_ALERT_UNIT_ALIAS]
            alert_unit_id = int(row[DataAccess.COLUMN_ALERT_UNIT_ID])
            metric_name = row[DataAccess.COLUMN_ALERT_METRIC_ALIAS]
            metric_id = int(row[DataAccess.COLUMN_ALERT_METRIC_ID])
            if alert_location_name is None:
                alert_location_name = row[DataAccess.COLUMN_ALERT_LOCATION_NAME]
            if alert_unit_name is None:
                alert_unit_name = '_'.join(row[DataAccess.COLUMN_ALERT_UNIT_NAME].split('_')[2:5])
            if metric_name is None:
                metric_name = '_'.join(row[DataAccess.COLUMN_ALERT_METRIC_NAME].split('_')[5:])
            
            alert_category = row[DataAccess.COLUMN_ALERT_CATEGORY]
            alert_type = row[DataAccess.COLUMN_ALERT_METRIC_TYPE]
            metric_target = row[DataAccess.COLUMN_ALERT_METRIC_TARGET]
            
            if row[DataAccess.COLUMN_ALERT_HAS_TARGET_VALUE] == 1:
                try:
                    metric_value_target = float(metric_target[:metric_target.index('(')])
                except ValueError:
                    try:
                        metric_value_target = float(metric_target.strip().split(' ')[0].strip())
                    except ValueError:
                        pass
            
                try:
                    metric_value_actual = float(row[DataAccess.COLUMN_ALERT_METRIC_VAL])
                except ValueError:
                    try:
                        metric_value_actual = float(row[DataAccess.COLUMN_ALERT_METRIC_VAL].strip().split(' ')[0].strip())
                    except ValueError:
                        pass
            
            #Percent deviation
            try:
                if metric_value_target != 0.0 and metric_value_target != config.STRING_NA:
                    P_dev = abs(metric_value_target - metric_value_actual) / metric_value_target
                if P_dev != config.STRING_NA:
                    P_dev = "{:.2f}%".format(P_dev*100.0)
            except:
                pass
            
            #Append to list of alerts
            alert_table.append({
                "alert_status": alert_status,
                "alert_category": alert_category,
                "alert_id": alert_id,
                "alert_type": alert_type,
                "alert_date": alert_datetime_str,
                "alert_eta_change_reason": alert_eta_change_reason,
                "alert_rect_action_change_reason": alert_rect_action_change_reason,
                
                "location_id": alert_location_id,
                "location_name": alert_location_name,
                "unit_id": alert_unit_id,
                "unit_name": alert_unit_name,
                "metric_id": metric_id,
                "metric_name": metric_name,
                
                "client": alert_client,
                
                "target": metric_value_target,
                "actual": metric_value_actual,
                "percent_dev": P_dev,
                
                "ack_by_id": ack_by_id,
                "ack_by_name": ack_by_name,
                "ack_date": ack_datetime_str,
                
                "rect_by_id": rect_by_id,
                "rect_by_name": rect_by_name,
                "rect_action": rect_action,
                "rect_action_id": rect_action_id,
                "expected_rect_time": expected_rect,
                "rect_date": rect_datetime_str,
                
                "ack_days": days_to_ack,
                "rect_days": days_to_rect
            })
        
        #Sorting from DB ??
        #alert_table.sort(key=lambda item: pd.to_datetime(item["alert_date"]), reverse = True)
        
        return alert_table
    
    def AlertStatusGrouper(self, alert):
        end_time = self.dal.totime
        cat_mask = 0
        #bit 0 set = ack
        #bit 1 set = nack
        #bit 2 set = rect
        #bit 3 set = nrect
        
        #These might be the most complicated filters ever produced in history
        
        #Ack = 1
        if (
            (alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_ACK) and (alert[self.dal.COLUMN_ALERT_METRIC_ACKDT] <= end_time)
        ) or (
            (alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_RECT) and (alert[self.dal.COLUMN_ALERT_METRIC_RECTDT] <= end_time)
        ): cat_mask |= 1
        
        #NAck = 2
        if (
            (alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_OPEN) or (
                (
                    alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_ACK
                ) and (
                    alert[self.dal.COLUMN_ALERT_METRIC_ACKDT] > end_time
                )
            )
            #or (
            #    (
            #        alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_RECT
            #    ) and (
            #        alert[self.dal.COLUMN_ALERT_METRIC_RECTDT] > end_time
            #    )
            #)
        ): cat_mask |= 2
        
        #Rect = 4
        if (
            (
                alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_RECT
            ) and (
                alert[self.dal.COLUMN_ALERT_METRIC_RECTDT] <= end_time
            )
        ): cat_mask |= 4
        
        #NRect = 8
        if (
            (alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_ACK) or
            (
                (
                    alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_ACK
                ) and (
                    alert[self.dal.COLUMN_ALERT_METRIC_ACKDT] > end_time
                )
            ) or (
                (
                    alert[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_RECT
                ) and (
                    alert[self.dal.COLUMN_ALERT_METRIC_RECTDT] > end_time
                )
            )
        ): cat_mask |= 8
        
        return cat_mask
    
    def get_group(self, g, key):
        if key in g.groups: return g.get_group(key)
        return pd.DataFrame()
    
    def alertSummaryFiltered(self, count = True, filters = config.ALERT_SUMMARY_DEFAULT):
        end_time, dtOnehrbefore, dtToday_zerohrs, dtFirst_of_month = self._subdivideTime()
        
        response = {}
        response[config.ALERT_SUMMARY_CURRENT] = end_time.isoformat()
        
        Alerts_Today = self._alertsInTimeframe(dtToday_zerohrs, end_time)
        before0H = self._alertsBeforeTime(dtToday_zerohrs)
        allAlertsOneH = self._alertsInTimeframe(dtOnehrbefore, end_time)
        Alerts_MonthToDate = self._alertsInTimeframe(dtFirst_of_month, end_time)
        
        if config.ALERT_SUMMARY_LAST1H in filters:
            response[config.ALERT_SUMMARY_LAST1H] = {'open': None, 'ack': None, 'rect': None}
            grp_oneh = allAlertsOneH.groupby(self.dal.COLUMN_ALERT_METRIC_STATUS)
            
            response[config.ALERT_SUMMARY_LAST1H]['open'] = self.get_group(grp_oneh, config.ALERT_STATUS_OPEN)
            if count: response[config.ALERT_SUMMARY_LAST1H]['open'] = len(response[config.ALERT_SUMMARY_LAST1H]['open'])
            response[config.ALERT_SUMMARY_LAST1H]['ack'] = self.get_group(grp_oneh, config.ALERT_STATUS_ACK)
            if count: response[config.ALERT_SUMMARY_LAST1H]['ack'] = len(response[config.ALERT_SUMMARY_LAST1H]['ack'])
            response[config.ALERT_SUMMARY_LAST1H]['rect'] = self.get_group(grp_oneh, config.ALERT_STATUS_RECT)
            if count: response[config.ALERT_SUMMARY_LAST1H]['rect'] = len(response[config.ALERT_SUMMARY_LAST1H]['rect'])
        
        if config.ALERT_SUMMARY_FROM0H in filters:
            response[config.ALERT_SUMMARY_FROM0H] = {'open': None, 'ack': None, 'rect': None}
            grp_0h = Alerts_Today.groupby(self.dal.COLUMN_ALERT_METRIC_STATUS)
            
            response[config.ALERT_SUMMARY_FROM0H]['open'] = self.get_group(grp_0h, config.ALERT_STATUS_OPEN)
            if count: response[config.ALERT_SUMMARY_FROM0H]['open'] = len(response[config.ALERT_SUMMARY_FROM0H]['open'])
            response[config.ALERT_SUMMARY_FROM0H]['ack'] = self.get_group(grp_0h, config.ALERT_STATUS_ACK)
            if count: response[config.ALERT_SUMMARY_FROM0H]['ack'] = len(response[config.ALERT_SUMMARY_FROM0H]['ack'])
            response[config.ALERT_SUMMARY_FROM0H]['rect'] = self.get_group(grp_0h, config.ALERT_STATUS_RECT)
            if count: response[config.ALERT_SUMMARY_FROM0H]['rect'] = len(response[config.ALERT_SUMMARY_FROM0H]['rect'])
        
        if config.ALERT_SUMMARY_OPEN0H in filters:
            response[config.ALERT_SUMMARY_OPEN0H] = {'open': None, 'ack': None}
            open_before_0h = before0H.loc[before0H[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_OPEN]
            
            grp_open_before_0h = open_before_0h.groupby(self.dal.COLUMN_ALERT_METRIC_STATUS)
            
            response[config.ALERT_SUMMARY_OPEN0H]['open'] = self.get_group(grp_open_before_0h, config.ALERT_STATUS_OPEN)
            if count: response[config.ALERT_SUMMARY_OPEN0H]['open'] = len(response[config.ALERT_SUMMARY_OPEN0H]['open'])
            response[config.ALERT_SUMMARY_OPEN0H]['ack'] = self.get_group(grp_open_before_0h, config.ALERT_STATUS_ACK)
            if count: response[config.ALERT_SUMMARY_OPEN0H]['ack'] = len(response[config.ALERT_SUMMARY_OPEN0H]['ack'])
        
        if config.ALERT_SUMMARY_AK2DAY in filters:
            response[config.ALERT_SUMMARY_AK2DAY] = Alerts_Today.loc[Alerts_Today[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_ACK]
            if count: response[config.ALERT_SUMMARY_AK2DAY] = len(response[config.ALERT_SUMMARY_AK2DAY])
        
        if config.ALERT_SUMMARY_RT2DAY in filters:
            response[config.ALERT_SUMMARY_RT2DAY] = Alerts_Today.loc[Alerts_Today[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_RECT]
            if count: response[config.ALERT_SUMMARY_RT2DAY] = len(response[config.ALERT_SUMMARY_RT2DAY])
        
        if config.ALERT_SUMMARY_OP2DAY in filters:
            response[config.ALERT_SUMMARY_OP2DAY] = Alerts_Today.loc[Alerts_Today[self.dal.COLUMN_ALERT_METRIC_STATUS] == config.ALERT_STATUS_OPEN]
            if count: response[config.ALERT_SUMMARY_OP2DAY] = len(response[config.ALERT_SUMMARY_OP2DAY])
        
        if config.ALERT_SUMMARY_MTD_ALERTS in filters:
            response[config.ALERT_SUMMARY_MTD_ALERTS] = Alerts_MonthToDate
            if count: response[config.ALERT_SUMMARY_MTD_ALERTS] = len(response[config.ALERT_SUMMARY_MTD_ALERTS])
        
        
        ackDF = pd.DataFrame()
        nackDF = pd.DataFrame()
        rectDF = pd.DataFrame()
        nrectDF = pd.DataFrame()
        int_ack_aging = {"0": pd.DataFrame(), "1": pd.DataFrame(), "2": pd.DataFrame(), "2+": pd.DataFrame()}
        int_nack_aging = {"0": pd.DataFrame(), "1": pd.DataFrame(), "2": pd.DataFrame(), "2+": pd.DataFrame()}
        int_rect_aging = {"0": pd.DataFrame(), "1": pd.DataFrame(), "2": pd.DataFrame(), "2+": pd.DataFrame()}
        int_nrect_aging = {"0": pd.DataFrame(), "1": pd.DataFrame(), "2": pd.DataFrame(), "2+": pd.DataFrame()}
        
        def alert_grpby_aging(alert, days_from, days_to):
            if pd.isnull(days_to):
                return "Unknown timestamp"
            if type(days_from) == str: days_from = alert[days_from]
            if type(days_to) == str: days_to = alert[days_to]
            return (days_to - days_from).days
        
        #I have to return the modified dataframe because of https://stackoverflow.com/a/11222835
        def setAgingGroup(target_aging_dict, inDF, outDF, colA, colB):
            outDF = pd.concat([outDF, inDF]).drop_duplicates().reset_index(drop=True)
            aging_groups = outDF.groupby(outDF.apply(lambda x: alert_grpby_aging(x, colA, colB), axis=1))
            
            for aging_number_of_days, aging_item in aging_groups:
                ag_grp = aging_groups.get_group(aging_number_of_days)
                if aging_number_of_days >= 0 and aging_number_of_days < 3:
                    target_df_idx = str(aging_number_of_days)
                    if target_df_idx in target_aging_dict.keys():
                        target_aging_dict[target_df_idx] = pd.concat([target_aging_dict[target_df_idx], ag_grp]).drop_duplicates().reset_index(drop=True)
                elif aging_number_of_days >= 3:
                    target_aging_dict["2+"] = pd.concat([target_aging_dict["2+"], ag_grp]).drop_duplicates().reset_index(drop=True)
            return outDF
        
        def countDFs(intervalDF, agingDF):
            if count: intervalDF = len(intervalDF)
            
            for days in agingDF:
                inputDays = (self.dal.totime - self.dal.fromtime).days
                
                if days == "2+": aging_days_show = inputDays > 2
                else: aging_days_show = inputDays >= int(days)
                
                if not aging_days_show: agingDF[days] = None
                if count and agingDF[days] is not None: agingDF[days] = len(agingDF[days])
            
            return intervalDF
        
        #Group and classify
        if len(Alerts_MonthToDate) > 0:
            pdGroups = Alerts_MonthToDate.groupby(Alerts_MonthToDate.apply(self.AlertStatusGrouper, axis=1))
            for key, item in pdGroups:
                grp = pdGroups.get_group(key)
                
                #Ack
                if key & 1 != 0:
                    ackDF = setAgingGroup(int_ack_aging, grp, ackDF, self.dal.COLUMN_ALERT_TIME, self.dal.COLUMN_ALERT_METRIC_ACKDT)
                #NAck
                if key & 2 != 0:
                    nackDF = setAgingGroup(int_nack_aging, grp, nackDF, self.dal.COLUMN_ALERT_TIME, end_time)
                #Rect, hence also ack
                if key & 4 != 0:
                    rectDF = setAgingGroup(int_rect_aging, grp, rectDF, self.dal.COLUMN_ALERT_TIME, self.dal.COLUMN_ALERT_METRIC_RECTDT)
                #NRect
                if key & 8 != 0:
                    nrectDF = setAgingGroup(int_nrect_aging, grp, nrectDF, self.dal.COLUMN_ALERT_METRIC_ACKDT, end_time)
        
        
        #If we need count instead of df
        ackDF = countDFs(ackDF, int_ack_aging)
        nackDF = countDFs(nackDF, int_nack_aging)
        rectDF = countDFs(rectDF, int_rect_aging)
        nrectDF = countDFs(nrectDF, int_nrect_aging)
        
        #Output the required fields
        if config.ALERT_SUMMARY_MTD_ACKS in filters:
            response[config.ALERT_SUMMARY_MTD_ACKS] = ackDF
        if config.ALERT_SUMMARY_MTD_NACKS in filters:
            response[config.ALERT_SUMMARY_MTD_NACKS] = nackDF
        if config.ALERT_SUMMARY_MTD_RECTS in filters:
            response[config.ALERT_SUMMARY_MTD_RECTS] = rectDF
        if config.ALERT_SUMMARY_MTD_NRECTS in filters:
            response[config.ALERT_SUMMARY_MTD_NRECTS] = nrectDF
        if config.ALERT_SUMMARY_AGING_ACK in filters:
            response[config.ALERT_SUMMARY_AGING_ACK] = int_ack_aging
        if config.ALERT_SUMMARY_AGING_NACK in filters:
            response[config.ALERT_SUMMARY_AGING_NACK] = int_nack_aging
        if config.ALERT_SUMMARY_AGING_RECT in filters:
            response[config.ALERT_SUMMARY_AGING_RECT] = int_rect_aging
        if config.ALERT_SUMMARY_AGING_NRECT in filters:
            response[config.ALERT_SUMMARY_AGING_NRECT] = int_nrect_aging
        
        return response
    
    def _alertsBeforeTime(self, time):
        alr_in_frame = self.dal.df[
            (self.dal.df[self.dal.COLUMN_ALERT_TIME] <= time)]
        return alr_in_frame
    
    def _alertsInTimeframe(self, fromtime, totime):
        alr_in_frame = self.dal.df[
            (self.dal.df[self.dal.COLUMN_ALERT_TIME] >= fromtime) &
            (self.dal.df[self.dal.COLUMN_ALERT_TIME] <= totime)]
        return alr_in_frame
    
    def _subdivideTime(self):
        current = self.dal.totime
        
        oneHBefore = current + timedelta(hours=-1)
        
        current_ist = utils.dt_local(current)
        start_ist = utils.dt_local(self.dal.fromtime)
        
        zeroH = current_ist.replace(hour=0, minute=0, second=0, microsecond=0).tz_convert('UTC')
        firstOfMonth = start_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0).tz_convert('UTC')
        
        return current, oneHBefore, zeroH, firstOfMonth
