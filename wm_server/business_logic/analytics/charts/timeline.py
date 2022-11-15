
#Common libraries
from ..common import config
from ..common import utils

from .aggregator import Aggregator, ChartData

#===Dependencies===

#Pandas
import pandas as pd

#Plot image
import plotly.express as plx

class Timeline(Aggregator):
    """
    Timeline based charts
    """
    
    LOGIC_TYPE = config.LOGIC_ACTIVE_HIGH
    STATE_HIGH = "open_time"
    STATE_LOW = "close_time"
    
    ADD_CHART_REPORT = True
    ExcelConditionFormat = False
    
    def _setTimeColumns(self):
        self.TIME_COLUMNS = [self.STATE_HIGH, self.STATE_LOW]
    
    def __init__(self, dal):
        self._setTimeColumns()
        super().__init__(dal)
        self.threshold = {'upper': None, 'lower': None}
        self.adjustThresholds()
    
    def _stateListTimeStr(state_list, time_params):
        for i in range(len(state_list)):
            for j in time_params:
                state_list[i][j] = None if state_list[i][j] is None else state_list[i][j].isoformat()
        
        return state_list
    
    def _generateStateTransitions(self):
        final_df = self.dal.df[[self.STATE_HIGH, self.STATE_LOW]]
        final_df['within_threshold'] = final_df.apply(
            lambda x: (
                (x[self.STATE_LOW] - x[self.STATE_HIGH]).seconds / 60
            ) <= self.dal.metadata['thresholds']['upper'] if self.dal.metadata['thresholds']['upper'] != None else True,
            axis = 'columns'
        )
        return final_df.sort_values(by=[self.STATE_HIGH])
    
    def aggregate(self, time_interval=None, alternateFormat=False, timestampstr=True):
        """
        Aggregation function
        alternateFormat and time_interval are unused
        """
        
        state_list = self._generateStateTransitions()
        
        for column in [self.STATE_HIGH, self.STATE_LOW]:
            state_list[column] = state_list[column].apply(lambda x: None if pd.isnull(x) else (x.isoformat() if timestampstr else x))
        
        res_data = ChartData()
        res_data.parseDAL(self.dal)
        res_data.setDataset(state_list.to_dict(orient='records'))
        res_data.set('threshold', self.threshold)
        res_data.setDatetimeFormat('iso' if timestampstr else 'same')
        
        return res_data.toJSON()
    
    def adjustThresholds(self):
        if self.dal.metadata["thresholds"] is not None:
            if self.dal.metadata["thresholds"]['upper'] != None:
                self.threshold['upper'] = self.dal.metadata["thresholds"]["upper"]
            if self.dal.metadata["thresholds"]['lower'] != None:
                self.threshold['lower'] = self.dal.metadata["thresholds"]["lower"]
    
    
    #==== Report generation ====
    
    def _getStateDFColumns(self):
        return self.STATE_HIGH if self.LOGIC_TYPE == config.LOGIC_ACTIVE_HIGH else self.STATE_LOW, self.STATE_LOW if self.LOGIC_TYPE == config.LOGIC_ACTIVE_HIGH else self.STATE_HIGH

    def _getReportColumns(self):
        return ["Rd.No","Open time","Close time","Duration","Above Threshold"], ["i","dt","dt","s","s"]

    #Chart title to reflect energy consumtion
    def _getChartTitle(self, data):
        return self._formatChartTitle(config.CHART_TITLE_TIMELINE, data)
    
    #Create a regular, single-plot figure
    def _makePlotlyFigure(self, title, df, period=None, labels=None):
        on_col, off_col = self._getStateDFColumns()
        
        fig = plx.timeline(df, x_start=on_col, x_end=off_col, range_x=period, y="Metric name", color="Metric name", text=labels)
        
        fig.update_layout(
            width = config.CHART_PLOTLY_WIDTH,
            height = config.CHART_PLOTLY_HEIGHT,
            autosize = False,

            title = {
                'text': title,
                'y': 0.9,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },

            legend_y = -0.15,
            legend_x = 0,
            legend_orientation='h',
        )
        
        #Make each bar a bit thinner, otherwise it fills the entire graph
        fig.update_traces(width=0.08, textposition="auto")
        
        return fig
    
    #Chart for PDF override
    def _generateImageChart(self, data):
        """
        Produces a figure using Plotly.
        Note: See config.py for Chart config
        """
        
        on_col, off_col = self._getStateDFColumns()
        
        #Create a DataFrame with new columns "Metric name" and "Duration"
        df = pd.DataFrame(data['dataset'])
        df["Metric name"] = data['metric_name']
        df["Duration"] = (df[off_col]-df[on_col]).apply(lambda x:config.STRING_NA if pd.isnull(x) else str(x.to_pytimedelta()))
        
        #Create the chart figure
        fig = self._makePlotlyFigure(self._getChartTitle(data), df, (data['from'], data['to']), "Duration")
        
        return fig
    
    def _generateReportTable(self, time_interval):
        """
        Creates data with some summary data for Report table.
        This highly depends on metric type, so it is overridden in child classes
        time_interval is unused
        """
        
        data_raw = self.aggregate(timestampstr=False)
        r_cols, r_dtypes = self._getReportColumns()
        
        metricValues = []
        states = []
        min_val, max_val = None, None
        for i, data in enumerate(data_raw['dataset']):
            logic_high, logic_low = self._getStateDFColumns()
            
            mtr_open_time = utils.dt_local(data[logic_high]).tz_localize(None)
            mtr_close_time = config.STRING_NA
            mtr_duration = config.STRING_NA
            mtr_isabove = config.STRING_NA
            mtr_threshold_state = config.STATE_NORMAL
            mtr_duration_end_latch_time = None
            
            if not pd.isnull(data[logic_low]):
                mtr_close_time = utils.dt_local(data[logic_low]).tz_localize(None)
                mtr_duration_end_latch_time = mtr_close_time
            
            if mtr_duration_end_latch_time != None:
                mtr_duration = str((mtr_duration_end_latch_time - mtr_open_time).round('S').to_pytimedelta())
            
            if 'within_threshold' in data.keys():
                mtr_isabove = "No" if data['within_threshold'] else "Yes"
                mtr_threshold_state = config.STATE_NORMAL if data['within_threshold'] else config.STATE_OOR
            
            row_data = [i + 1, mtr_open_time, mtr_close_time, mtr_duration, mtr_isabove]
            
            metricValues.append(row_data)
            states.append(mtr_threshold_state)
        
        return data_raw, r_cols, metricValues, states, {'minval': min_val, 'maxval': max_val, 'dtypes': r_dtypes}
    
    def _generateExcelChart(self, workbook, worksheet,
        data_raw,
        DataPoint_Styles,
        options,
        limits,
        table_pos,
        time_interval):
        """
        Creates an excel chart for reports
        """
        
        #control_limits = limits[:2]
        #thresholds = limits[2:]
        #print(thresholds)
        
        #Min and Max for Y-axis (NOTE: Only timeseries)
        #min_value = options[0]
        #if min_value is None: min_value = 0
        #max_value = options[1]
        #if max_value is None: max_value = 0
        #min_value = min(min_value, control_limits[1], thresholds[1] if thresholds[1] != config.STRING_NA else min_value)
        #max_value = max(max_value, control_limits[0], thresholds[0] if thresholds[0] != config.STRING_NA else max_value)
        #mm_diff = max_value - min_value
        #min_value -= mm_diff * 0.10     #Leave a ~10% margin between peak points and chart area
        #max_value += mm_diff * 0.10
        #min_value = round(min_value)
        #max_value = round(max_value)
        
        #Create a chart object
        chart = workbook.add_chart({'type': 'scatter'})
        
        lineColors = [config.CHART_LINE_COLOR]
        lineXPoints = "={3, 6, 9, 12, 15}"#"={%s, %s}" % (data_raw['dataset'][0]['open_time'], data_raw['dataset'][0]['close_time'])
        lineYPoints = "={1,1,1,1,1}"
        
        #Main series
        chart.add_series({
            'name': data_raw['metric_name'],
            'categories': lineXPoints,#['Sheet1', table_pos[1], table_pos[0] + 1, table_pos[2], table_pos[0] + 1],
            'values': lineYPoints,
            #'points': [{'color': 'red', 'transparency': 1}],
            #'line': {'color': 'red', 'width': 30},
            'marker': {'type': 'none'}
        })
        
        #Chart setup
        chart.set_title({'name': self._getChartTitle(data_raw)})
        #chart.set_x_axis({'date_axis': True, 'num_format': config.CHART_TICKS_TIME_FORMAT, 'num_font': {'rotation': -45}})
        chart.set_size({'x_scale': config.CHART_SCALE[0], 'y_scale': config.CHART_SCALE[1]})
        chart.set_legend({'position': 'bottom'})
        
        #TODO: Disabled it for now since timeline charts in Excel basically don't exist.
        #HACK: Use Horizontal stacked bar graph to simulate a timeline chart
        
        #Add it just below the data table
        #worksheet.insert_chart(table_pos[2] + 2, 0, chart)



#==============================
# Endpoint classes for metrics
#==============================

class Door(Timeline):
    pass

class WaterTank(Timeline):
    LOGIC_TYPE = config.LOGIC_ACTIVE_LOW
    STATE_HIGH = "high_time"
    STATE_LOW = "low_time"
    
    def _getReportColumns(self):
        return ["Rd.No","Low time","High time","Duration","Above Threshold"], ["i","dt","dt","s","s"]
    
    def _generateStateTransitions(self):
        final_df = self.dal.df[[self.STATE_LOW, self.STATE_HIGH]]
        final_df['low_tank'] = True
        #final_df['low_tank'] = final_df.apply(lambda x: (True if(x[self.STATE_LOW] > x[self.STATE_HIGH]) else False), axis = 'columns') 
        #final_df.apply(lambda x: (
        #        (
        #            (x[self.STATE_HIGH] - x[self.STATE_LOW]).seconds / 60 > self.threshold['upper']
        #            if self.threshold['upper'] != None
        #            else False
        #        )
        #    ),
        #    axis = 'columns'
        #)
        return final_df.sort_values(by=[self.STATE_LOW])


class Panic(Timeline):
    STATE_LOW = "rectified_time"

class VESDA(Timeline):
    STATE_HIGH = "vesda_active_time"
    STATE_LOW = "vesda_inactive_time"
    
    def _setTimeColumns(self):
        self.TIME_COLUMNS = ["measure_time"]
    
    def __init__(self, dal):
        dal.COLUMN_METRIC_RDG_TIME = "measure_time"
        dal.COLUMN_METRIC_RDG_VALUE = "vesda_value"
        super().__init__(dal)
    
    def _generateStateTransitions(self):
        n_data = []
        cur_row=None
        srt_df=self.dal.df.sort_values(by=[self.dal.COLUMN_METRIC_RDG_TIME])
        for idx,row in srt_df.iterrows():
            #If the state is 1, start a new row
            if row[self.dal.COLUMN_METRIC_RDG_VALUE]==1:
                cur_row=[row[self.dal.COLUMN_METRIC_RDG_TIME], None, True]
            else:
                if cur_row is not None:
                    cur_row[1] = row[self.dal.COLUMN_METRIC_RDG_TIME]
                    cur_row[2] = (
                        (cur_row[1] - cur_row[0]).seconds / 60
                    ) <= self.threshold['upper'] if self.threshold['upper'] != None else True
                    n_data.append(cur_row)
                    cur_row=None

        #If the last state is not 0, we still have an incomplete row, so append it
        if cur_row is not None:
            n_data.append(cur_row)
        
        final_df = pd.DataFrame(n_data, columns=[self.STATE_HIGH, self.STATE_LOW, 'within_threshold'])
        
        return final_df.sort_values(by=[self.STATE_LOW])
