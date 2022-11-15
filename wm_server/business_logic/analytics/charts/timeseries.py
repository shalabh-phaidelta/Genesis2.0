
#Common libraries
from ..common import config
from ..common import utils

from datetime import timedelta

from .aggregator import Aggregator

#===Dependencies===

#Pandas
import pandas as pd

#Plot image
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class Timeseries(Aggregator):
    """
    Line based charts
    """
    
    DoAverage = True
    Cumulative = False
    SwapColumns = False
    
    def __init__(self, dal):
        self.TIME_COLUMNS = [dal.COLUMN_METRIC_RDG_TIME]
        
        super().__init__(dal)
        self.dal.df = self.dal.df.drop_duplicates(self.dal.COLUMN_METRIC_RDG_TIME).set_index(self.dal.COLUMN_METRIC_RDG_TIME)
    
    def aggregate(self, time_interval, alternateFormat=False, timestampstr=True):
        """
        Aggregation function
        Performs a database load and returns an object with datapoints.
        alternateFormat will group "X", "Y" and "State" points together
        timestampstr will return all timestamps as string instead of pandas' Timestamp object
        
        Cumulative will also append a cumulative value column
        """
        
        dataPoints = []
        out_df = None
        jsonData = {}
        self.time_interval = time_interval
        self.threshold = {'upper': None, 'lower': None}
        
        #Make adjustments to input parameters if needed
        self.adjustAggregationColumnUsage()
        self.adjustThresholds()
        
        #Type-cast to float
        self.dal.df[self.AGGREGATION_COLUMN] = self.dal.df[self.AGGREGATION_COLUMN].astype(float)
        
        dataPoints = self.generateDataPoints(timestampstr)
        out_df = self.dataPointsToDF(dataPoints, timestampstr)
        jsonData = self.generateJSON(out_df, timestampstr)
        
        return jsonData
    
    def adjustAggregationColumnUsage(self):
        #Custom tolerances
        if self.time_interval == config.METRIC_INTERVAL_AGGREGATE['15M']:
            if self.SwapColumns:
                self.AGGREGATE_DB_INTERVAL = self.time_interval
                self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_VALUE
        elif self.time_interval == config.METRIC_INTERVAL_AGGREGATE['30M']:
            if self.SwapColumns:
                self.AGGREGATE_DB_INTERVAL = self.time_interval
                self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_VALUE_30M
        elif self.time_interval == config.METRIC_INTERVAL_AGGREGATE['1H']:
            if self.SwapColumns:
                self.AGGREGATE_DB_INTERVAL = self.time_interval
                self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_VALUE_60M
            self.NEAREST_POINT_TOLERANCE = config.METRIC_INTERVAL_AGGREGATE['15M']
        elif self.time_interval == config.METRIC_INTERVAL_AGGREGATE['1D']:
            if self.SwapColumns:
                self.AGGREGATE_DB_INTERVAL = self.time_interval
                self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_VALUE_24H
            self.NEAREST_POINT_TOLERANCE = config.METRIC_INTERVAL_AGGREGATE['1H']
    
    def adjustThresholds(self):
        if self.dal.metadata["thresholds"] is not None:
            if self.dal.metadata["thresholds"]['upper'] != None:
                self.threshold['upper'] = self.dal.metadata["thresholds"]["upper"]
            if self.dal.metadata["thresholds"]['lower'] != None:
                self.threshold['lower'] = self.dal.metadata["thresholds"]["lower"]
            
            #Adjust thresholds as aggregation interval changes (N/A for averaging)
            if not self.DoAverage:
                scale_threshold_factor = (self.time_interval // self.THRESHOLD_FOR_INTERVAL)
                for x in self.threshold:
                    self.threshold[x] = self.threshold[x] * scale_threshold_factor if self.threshold[x] != None else None
    
    
    #Local time (client) End of Day list
    def makeDayIterator(self, from_ts, to_ts):
        int_beginning_list = []
        
        #Localize the interval times
        from_ts = utils.dt_local(from_ts)
        to_ts = utils.dt_local(to_ts)
        
        first_start_ts = from_ts
        first_end_ts = from_ts.replace(hour=0, minute=0, second=0, microsecond=0) + config.METRIC_INTERVAL_AGGREGATE['1D']
        
        
        final_start_ts = to_ts.replace(hour=0, minute=0, second=0, microsecond=0)
        final_start_ts = final_start_ts - config.METRIC_INTERVAL_AGGREGATE['1D'] if to_ts == final_start_ts else final_start_ts
        final_end_ts = to_ts
        
        if self.DoAverage:
            #Starting offset check
            start_off_from_db_interval = utils.td_get_total_seconds(first_end_ts-first_start_ts) % utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL)
            if start_off_from_db_interval > utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) // 2:
                first_start_ts -= utils.td_sec(utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) - start_off_from_db_interval)
            else:
                first_start_ts += utils.td_sec(start_off_from_db_interval)
            
            #Ending offset check
            end_off_from_db_interval = utils.td_get_total_seconds(final_end_ts-final_start_ts) % utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL)
            if end_off_from_db_interval > utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) // 2:
                final_end_ts += utils.td_sec(utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) - end_off_from_db_interval)
            else:
                final_end_ts -= utils.td_sec(end_off_from_db_interval)
        
        
        int_beginning_list.append((first_start_ts, first_end_ts))
        int_beginning_list.append((final_start_ts, final_end_ts))
        
        
        intv = pd.date_range(first_end_ts, final_start_ts, freq=config.METRIC_INTERVAL_AGGREGATE['1D'], normalize=True, closed='left')
        for x in intv:
            int_beginning_list.append((
                x,
                x + config.METRIC_INTERVAL_AGGREGATE['1D']
            ))
        
        #if midnight of to_ts == to_ts:
        #    to_ts = midnight of to_ts - 1D
        #
        #intv = pd.date_range(utils.dt_local(to_ts), utils.dt_local(from_ts), freq=-config.METRIC_INTERVAL_AGGREGATE['1D'])
        #
        #print('='*10)
        #print(intv)
        #print('='*10)
        #
        #int_beginning_list = []
        #
        #for i, x in enumerate(intv):
        #    print('='*5)
        #    print(x)
        #    print('='*5)
        #    
        #    #End date
        #    if i == 0:
        #        end_midnight = x.replace(hour=0, minute=0, second=0, microsecond=0)
        #        
        #        end_interval = [end_midnight, to_ts]
        #        
        #        
        #        print("End interval:")
        #        print(end_interval)
        #        
        #        if self.DoAverage:
        #            end_off_from_db_interval = utils.td_get_total_seconds(end_interval[1]-end_interval[0]) % utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL)
        #            if end_off_from_db_interval > utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) // 2:
        #                end_interval[1] += utils.td_sec(utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) - end_off_from_db_interval)
        #            else:
        #                end_interval[1] -= utils.td_sec(end_off_from_db_interval)
        #        
        #        int_beginning_list.append(tuple(end_interval))
        #    #Start date
        #    elif i == len(intv)-1:
        #        start_interval = [utils.dt_local(from_ts), utils.dt_local(from_ts).replace(hour=0, minute=0, second=0, microsecond=0) + config.METRIC_INTERVAL_AGGREGATE['1D']]
        #        
        #        print("Start interval:")
        #        print(start_interval)
        #        
        #        if self.DoAverage:
        #            start_off_from_db_interval = utils.td_get_total_seconds(start_interval[1]-start_interval[0]) % utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL)
        #            if start_off_from_db_interval > utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) // 2:
        #                start_interval[0] -= utils.td_sec(utils.td_get_total_seconds(self.AGGREGATE_DB_INTERVAL) - start_off_from_db_interval)
        #            else:
        #                start_interval[0] += utils.td_sec(start_off_from_db_interval)
        #        
        #        int_beginning_list.append(tuple(start_interval))
        #    #Anything in-between
        #    else:
        #        int_beginning_list.append((x.replace(hour=0, minute=0, second=0, microsecond=0), x.replace(hour=0, minute=0, second=0, microsecond=0) + config.METRIC_INTERVAL_AGGREGATE['1D']))
        #
        #print('='*10)
        #
        return int_beginning_list
    
    
    def generateDataPoints(self, timestampstr):
        dataPoints = []
        aggRangeStart = self.dal.fromtime
        aggRangeEnd = self.dal.totime
        lowestTime, highestTime = None, None
        
        #If selected time does not have a point, snap to nearest point
        #try:
        #    aggRangeEnd = self.dal.df.iloc[self.dal.df.index.get_loc(aggRangeEnd, method='nearest', tolerance='1T')]
        #except KeyError:
        #    aggRangeEnd = self.dal.df.index[0]
        
        if self.time_interval != config.METRIC_INTERVAL_AGGREGATE['1D']:
            date_iter = [(x - self.time_interval, x) for x in pd.date_range(aggRangeEnd, aggRangeStart, freq=-self.time_interval)]
        else:
            date_iter = self.makeDayIterator(aggRangeStart, aggRangeEnd)
        
        #Actual calculation
        for itr_time_interval_start, itr_time_interval_end in date_iter:
            #midpoint = itr_time_interval_end - self.time_interval / 2
            midpoint = itr_time_interval_start + (itr_time_interval_end - itr_time_interval_start) / 2
            
            #If "By accident" midpoint crosses the time range
            if midpoint < aggRangeStart or (self.time_interval != config.METRIC_INTERVAL_AGGREGATE['1D'] and itr_time_interval_start < aggRangeStart):
                break
            
            yValue, tCount, minmax_interval, ptsSummed = self._calculateSumBetweenFrame(itr_time_interval_start, itr_time_interval_end, self.time_interval)
            
            if tCount > 0:
                if self.DoAverage:
                    yValue /= tCount
                
                pointX = midpoint
                pointY = round(yValue, config.ROUND_DECIMAL_PTS)
                pointS = config.STATE_NORMAL
                
                subIterMin = min([x[0] for x in ptsSummed])
                subIterMax = max([x[0] for x in ptsSummed])
                
                if self.DoAverage:
                    subIterMin -= self.AGGREGATE_DB_INTERVAL
                
                if lowestTime == None: lowestTime = subIterMin
                else: lowestTime = min(lowestTime, subIterMin)
                
                if highestTime == None: highestTime = subIterMax
                else: highestTime = max(highestTime, subIterMax)
                
                
                #Don't let slight round errors pass
                min_val_interval, max_val_interval = minmax_interval
                if not pd.isnull(min_val_interval):
                    min_val_interval = float(min_val_interval)
                    min_val_interval = min(min_val_interval, pointY)
                if not pd.isnull(max_val_interval):
                    max_val_interval = float(max_val_interval)
                    max_val_interval = max(max_val_interval, pointY)
                
                if self.dal.metadata["thresholds"] != None:
                    if not utils.isWithinBoundary(pointY, self.threshold["lower"], self.threshold["upper"]):
                        pointS = config.STATE_OOR
                
                p = {
                    "x": pointX,
                    "y": pointY,
                    "state": pointS,
                    "minmax": {
                        "min": min_val_interval,
                        "max": max_val_interval
                    },
                    "subTime": {
                        "min": subIterMin.isoformat() if timestampstr else subIterMin,
                        "max": subIterMax.isoformat() if timestampstr else subIterMax
                    }
                }
                
                dataPoints.append(p)
        
        
        #Re-adjust start and end time (if changed)
        if lowestTime != None: self.dal.fromtime = utils.dt_local(lowestTime)
        if highestTime != None: self.dal.totime = utils.dt_local(highestTime)
        
        return dataPoints
    
    def dataPointsToDF(self, dataPoints, timestampstr):
        #Create a pandas Dataframe
        out_df = pd.DataFrame(dataPoints)
        if len(out_df) > 0:
            out_df['x'] = out_df['x'].apply(lambda x: x.tz_convert('UTC'))
            if timestampstr: out_df['x'] = out_df['x'].apply(lambda x: x.isoformat())
            out_df = out_df.sort_values(by=['x'])
            out_df['y'] = out_df['y'].round(config.ROUND_DECIMAL_PTS)
            
            if self.Cumulative:
                oldest_dataPoint = out_df[out_df['x'] == out_df['x'].min()]['y2_start']
                out_df['y2'] = out_df['y2_end'].apply(lambda x: x-oldest_dataPoint).round(config.ROUND_DECIMAL_PTS)
        
        return out_df
    
    
    def generateJSON(self, df, timestampstr):
        #Y-axis unit, if available
        try:
            measure_unit = config.METRIC_UNIT[self.dal.metadata["metric_type"]]
        except KeyError:
            measure_unit = None
        except ValueError:
            measure_unit = None
        try:
            valid_intervals = config.METRIC_INTERVALS[self.dal.metadata["metric_type"]]
        except KeyError:
            valid_intervals = None
        except ValueError:
            valid_intervals = None
        
        #TODO: Make use of ChartData structure generation class, as used in timeline.py
        
        mtr_label = self.dal.metadata['metric_name']
        mtr_iswhlvl = int(self.dal.metadata['iswhlunit'])
        loc_name = self.dal.metadata['location_alias']
        unit_name = self.dal.metadata['unit_alias']
        mtr_name = self.dal.metadata['metric_alias']
        if loc_name == None: loc_name = self.dal.metadata['location_name']
        if unit_name == None: unit_name = '_'.join(self.dal.metadata['unit_name'].split('_')[2:5])
        if mtr_name == None: mtr_name = '_'.join(self.dal.metadata['metric_name'].split('_')[5:])
        
        #Construct the JSON Structure
        jsonData = {
            "from": self.dal.fromtime.isoformat() if timestampstr else self.dal.fromtime,
            "to": self.dal.totime.isoformat() if timestampstr else self.dal.totime,
            "interval": utils.td_get_total_seconds(self.time_interval) if timestampstr else self.time_interval,
            "intervals": valid_intervals,
           
            "metric_id": self.dal.metadata['metric_id'],
            "metric_name": mtr_name,
            "location_id": self.dal.metadata['location_id'],
            "location_name": loc_name,
            "unit_id": self.dal.metadata['unit_id'],
            "unit_name": unit_name,
            
            self.dal.COLUMN_METRIC_IS_WHL_MTR: mtr_iswhlvl,
            
            "threshold": self.threshold,
            "control_limits": {"UCL": None, "LCL": None},
            "mean": None,
            "stdev": None,
            
            "measure_unit": measure_unit,
            "label": mtr_label,
            "dataset": df.to_dict(orient='records')
        }
        
        if len(df) > 0:
            y_points = df['y']
            if len(y_points) > 0:
                jsonData["mean"] = y_points.mean()
            if len(y_points) > 1:
                jsonData["stdev"] = y_points.std()
                
                jsonData["control_limits"] = {
                    "LCL": max(0, round(jsonData["mean"] - 3.0 * jsonData["stdev"], config.ROUND_DECIMAL_PTS)),
                    "UCL": round(jsonData["mean"] + 3.0 * jsonData["stdev"], config.ROUND_DECIMAL_PTS)
                }
        return jsonData
    
    
    #==== Report generation ====
    
    def _getReportColumns(self, avg_col="Avg"):
        return ["Rd.No","Start time","End time",avg_col,"Min","Max"], ["i", "dt", "dt", "f", "f", "f"]
    
    #Create a regular, single-plot figure
    def _makePlotlyFigure(self, data):
        fig = go.Figure()
        fig.update_layout(
            title = {
                'text': self._getChartTitle(data),
                'x':0.5,
                'y':0.95,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            width = config.CHART_PLOTLY_WIDTH,
            height = config.CHART_PLOTLY_HEIGHT,
            autosize=False
        )
        fig.update_xaxes(rangemode="tozero")
        fig.update_yaxes(rangemode="tozero", automargin=True)
        return fig
    
    #Render image chart using matplotlib
    def _generateImageChart(self, data):
        """
        Produces a figure using Plotly.
        Note: See config.py for Chart config
        """
        
        fig = self._makePlotlyFigure(data)
        
        x_points, y_points, m_colors = [], [], []
        ucl_points, lcl_points = [], []
        utl_points, ltl_points = [], []
        
        MTR_LTL, MTR_UTL = None, None
        if data["threshold"] is not None:
            if data["threshold"]["lower"] is not None:
                MTR_LTL = float(data["threshold"]["lower"])
            if data["threshold"]["upper"] is not None:
                MTR_UTL = float(data["threshold"]["upper"])
        
        label = self._getYAxisLabel(data)
        
        for x in data['dataset']:
            x_points.append(utils.dt_local(x['x']))
            y_points.append(x['y'])
            m_colors.append(config.CHART_MARKER_COLOR_NORMAL if x['state'] == config.STATE_NORMAL else config.CHART_MARKER_COLOR_OOR)
            ucl_points.append(data['control_limits']['UCL'])
            lcl_points.append(data['control_limits']['LCL'])
            utl_points.append(MTR_UTL)
            ltl_points.append(MTR_LTL)
        
        #Begin plotting
        fig.update_layout(
            template='plotly_white',
            
            xaxis_linecolor='black',
            yaxis_linecolor='black',
            
            xaxis_mirror=True,
            yaxis_mirror=True,
            
            xaxis_showgrid=True,
            yaxis_showgrid=True,
            
            xaxis_showline=True,
            yaxis_showline=True,
            
            xaxis_title = 'Time',
            yaxis_title = label,
            
            xaxis_tickformat = config.CHART_XAXIS_FORMAT_PLOTLY,
            
            legend_x = 0,
            legend_y = -0.15,
            legend_orientation='h'
        )
        
        tinterval = utils.td_get_total_seconds(data['interval']) * 1000
        
        fig.update_xaxes(dtick=tinterval, ticks="inside")
        fig.update_yaxes(ticks="inside")
        
        #Main series
        fig.add_trace(go.Scatter(
            x=x_points,
            y=y_points,
            text=y_points,
            textposition="top center",
            
            name=data['label'],
            line_width=config.CHART_LINE_THICKNESS,
            line_shape="spline",
            line_color=config.CHART_LINE_COLOR,
            mode="markers+lines+text",
            marker=go.scatter.Marker(
                size=config.CHART_MARKER_RADIUS*1.5,
                color=m_colors,
                opacity=.5,
                colorscale="Viridis"
            )
        ))
        
        #UCL
        fig.add_trace(go.Scatter(
            x=x_points,
            y=ucl_points,
            
            name="UCL",
            line_width=config.CHART_CONTROLLIMIT_THICKNESS,
            line_color=config.CHART_CONTROLLIMIT_COLOR,
            line_dash=config.CHART_CONTROLLIMIT_DASHTYPE[1],
            mode="lines"
        ))
        
        #LCL
        fig.add_trace(go.Scatter(
            x=x_points,
            y=lcl_points,
            
            name="LCL",
            line_width=config.CHART_CONTROLLIMIT_THICKNESS,
            line_color=config.CHART_CONTROLLIMIT_COLOR,
            line_dash=config.CHART_CONTROLLIMIT_DASHTYPE[1],
            mode="lines"
        ))
        
        if MTR_UTL is not None:
            #UTL
            fig.add_trace(go.Scatter(
                x=x_points,
                y=utl_points,
                
                name="Upper Limit",
                line_width=config.CHART_THRESHOLD_THICKNESS,
                line_color=config.CHART_THRESHOLD_COLOR,
                mode="lines"
            ))
        
        if MTR_LTL is not None:
            #LTL
            fig.add_trace(go.Scatter(
                x=x_points,
                y=ltl_points,
                
                name="Lower Limit",
                line_width=config.CHART_THRESHOLD_THICKNESS,
                line_color=config.CHART_THRESHOLD_COLOR,
                mode="lines"
            ))
        
        return fig
    
    def _generateReportTable(self, time_interval):
        """
        Creates data with some summary data for Report table.
        This highly depends on metric type, so it is overridden in
        child classes
        """
        
        data_raw = self.aggregate(time_interval, timestampstr=False)
        
        label = "Avg"
        if data_raw['measure_unit'] is not None:
            if data_raw['measure_unit'][0] != '' and data_raw['measure_unit'][0] is not None:
                label += ' ({})'.format(data_raw['measure_unit'][0])
        
        r_cols, r_dtypes = self._getReportColumns(label)
        
        metricValues = []
        states = []
        min_val, max_val = None, None
        for i, data in enumerate(data_raw['dataset']):
            start_time = data['subTime']['min']
            end_time = data['subTime']['max']
            
            val_y = data['y']
            min_val = val_y if min_val is None else min(min_val, val_y)
            max_val = val_y if max_val is None else max(max_val, val_y)
            
            try:
                min_per_rdg, max_per_rdg = data['minmax']['min'], data['minmax']['max']
                if min_per_rdg == None: min_per_rdg = config.STRING_NA
                if max_per_rdg == None: max_per_rdg = config.STRING_NA
            except IndexError:
                #Shouldn't happen
                min_per_rdg, max_per_rdg = config.STRING_NA, config.STRING_NA
            
            #Since this is for a report, we convert to localtime from UTC
            #Also excel hates the fact that a date contains a timezone, so we strip that away
            row_data = [
                i + 1,
                utils.dt_local(start_time).tz_localize(None),
                utils.dt_local(end_time).tz_localize(None),
                val_y,
                min_per_rdg,
                max_per_rdg
            ]
            metricValues.append(row_data)
            states.append(data['state'])
        
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
        
        control_limits = limits[:2]
        thresholds = limits[2:]
        
        #Min and Max for Y-axis (NOTE: Only timeseries)
        max_value = options['maxval']
        if max_value is None: max_value = 0
        
        min_value = 0
        max_value = max(max_value, control_limits[0] if control_limits[0] != config.STRING_NA else max_value)
        max_value = max(max_value, thresholds[0] if thresholds[0] != config.STRING_NA else max_value)
        
        max_value += max_value * 0.10
        
        max_value = round(max_value, 2)
        
        #Create a chart object
        chart = workbook.add_chart({'type': 'scatter', 'subtype': 'smooth_with_markers'})
        
        d_points = data_raw['dataset']
        
        category_column = 1
        value_column = 3
        
        cat_time_points = [utils.excel_date(utils.dt_local(x['x']).tz_localize(None)) for x in d_points]
        interval_days = utils.td_get_total_seconds(data_raw['interval']) / config.SECONDS_IN_DAY    #86400 Seconds in a day
        category_min = utils.excel_date(utils.dt_local(data_raw['from']).tz_localize(None))
        category_max = utils.excel_date(utils.dt_local(data_raw['to']).tz_localize(None))
        cat_time_points = [str(x) for x in cat_time_points]
        category_formula = "={" + ','.join(cat_time_points) + "}"
        
        #category_formula = ['Sheet1', table_pos[1], table_pos[0] + category_column, table_pos[2], table_pos[0] + category_column]
        
        #Main series
        chart.add_series({
            'name': data_raw['label'],
            'values': ['Sheet1', table_pos[1], table_pos[0] + value_column, table_pos[2], table_pos[0] + value_column],
            'categories': category_formula,
            'points': DataPoint_Styles,
            'line': {'color': config.CHART_LINE_COLOR, 'width': config.CHART_LINE_THICKNESS},
            'marker': {
                'type': config.CHART_MARKER_KIND,
                'size': config.CHART_MARKER_RADIUS,
                'border': {'color': 'black', 'width': 1}
            },
            'data_labels': {'value': True, 'position': 'above'},
        })
        
        #UCL
        chart.add_series({
            'name': "UCL",
            'values': utils.makeFormulaOfValuesRepeated(str(control_limits[0]), len(data_raw['dataset'])),
            'categories': category_formula,
            'marker': {'type': 'none'},
            'line': {'color': config.CHART_CONTROLLIMIT_COLOR, 'width': config.CHART_CONTROLLIMIT_THICKNESS, 'dash_type': config.CHART_CONTROLLIMIT_DASHTYPE[0]}
        })
        
        #LCL
        chart.add_series({
            'name': "LCL",
            'values': utils.makeFormulaOfValuesRepeated(str(control_limits[1]), len(data_raw['dataset'])),
            'categories': category_formula,
            'marker': {'type': 'none'},
            'line': {'color': config.CHART_CONTROLLIMIT_COLOR, 'width': config.CHART_CONTROLLIMIT_THICKNESS, 'dash_type': config.CHART_CONTROLLIMIT_DASHTYPE[0]}
        })
        
        #UTL
        if thresholds[0] != config.STRING_NA:
            chart.add_series({
                'name': "Upper Limit",
                'values': utils.makeFormulaOfValuesRepeated(str(thresholds[0]), len(data_raw['dataset'])),
                'categories': category_formula,
                'marker': {'type': 'none'},
                'line': {'color': config.CHART_THRESHOLD_COLOR, 'width': config.CHART_THRESHOLD_THICKNESS}
            })
        
        #LTL
        if thresholds[1] != config.STRING_NA:
            chart.add_series({
                'name': "Lower Limit",
                'values': utils.makeFormulaOfValuesRepeated(str(thresholds[1]), len(data_raw['dataset'])),
                'categories': category_formula,
                'marker': {'type': 'none'},
                'line': {'color': config.CHART_THRESHOLD_COLOR, 'width': config.CHART_THRESHOLD_THICKNESS}
            })
        
        #Chart setup
        label = self._getYAxisLabel(data_raw)
        chart.set_title({'name': self._getChartTitle(data_raw)})
        chart.set_x_axis({
            'name': 'Time',
            'date_axis': True,
            'num_format': config.CHART_TICKS_TIME_FORMAT,
            'min': category_min,
            'max': category_max,
            'major_unit': interval_days,
            'num_font': {'rotation': -45}
        })
        chart.set_y_axis({'name': label, 'min': min_value, 'max': max_value})#, 'major_unit': 2
        chart.set_size({'x_scale': config.CHART_SCALE[0], 'y_scale': config.CHART_SCALE[1]})
        chart.set_legend({'position': 'bottom'})
        
        
        #Add it just below the data table
        worksheet.insert_chart(table_pos[2] + 2, 0, chart)


class Cumulative(Timeseries):
    def __init__(self, dal):
        super().__init__(dal)
        
        self.DoAverage = False
        self.Cumulative = True
        self.SwapColumns = False
    
    def _getDataAtPoint(self, midpoint, itr_time_interval_start, itr_time_interval_end, time_interval):
        pointFound = False
        rdg_val_start = None
        rdg_val_end = None
        
        try:
            #print(self.dal.df[0:7])
            df_aggregate_at_start = self.dal.df.iloc[self.dal.df.index.get_loc(itr_time_interval_start, method='nearest', tolerance=self.NEAREST_POINT_TOLERANCE)]
            df_aggregate_at_end = self.dal.df.iloc[self.dal.df.index.get_loc(itr_time_interval_end, method='nearest', tolerance=self.NEAREST_POINT_TOLERANCE)]
            rdg_val_start = df_aggregate_at_start[self.AGGREGATION_COLUMN]
            rdg_val_end = df_aggregate_at_end[self.AGGREGATION_COLUMN]
            pointFound = True
        except KeyError:
            pass
        except IndexError:
            pass
        
        return rdg_val_start, rdg_val_end, pointFound
    
    #Override to change logic for Energy, using instantaneous
    def generateDataPoints(self, timestampstr):
        dataPoints = []
        aggRangeStart = self.dal.fromtime
        aggRangeEnd = self.dal.totime
        lowestTime, highestTime = None, None
        
        #If selected time does not have a point, snap to nearest point
        #try:
        #    aggRangeEnd = self.dal.df.iloc[self.dal.df.index.get_loc(aggRangeEnd, method='nearest', tolerance='1T')]
        #except KeyError:
        #    aggRangeEnd = self.dal.df.index[0]
        
        if self.time_interval != config.METRIC_INTERVAL_AGGREGATE['1D']:
            date_iter = [(x - self.time_interval, x) for x in pd.date_range(aggRangeEnd, aggRangeStart, freq=-self.time_interval)]
        else:
            date_iter = self.makeDayIterator(aggRangeStart, aggRangeEnd)
        
        #Actual calculation
        for itr_time_interval_start, itr_time_interval_end in date_iter:
            #midpoint = itr_time_interval_end - self.time_interval / 2
            midpoint = itr_time_interval_start + (itr_time_interval_end - itr_time_interval_start) / 2
            
            rdg_val_start = None
            rdg_val_end = None
            
            
            #If "By accident" midpoint crosses the time range
            if midpoint < aggRangeStart or (self.time_interval != config.METRIC_INTERVAL_AGGREGATE['1D'] and itr_time_interval_start < aggRangeStart):
                #print("EXIT??")
                #print("Please DEBUG!")
                #print("Midpoint:", midpoint)
                #print("Start time entered:", aggRangeStart)
                #print("Interval start/end:", itr_time_interval_start, itr_time_interval_end)
                break
            
            #try:
            #    df_aggregate_at_start = self.dal.df.iloc[self.dal.df.index.get_loc(itr_time_interval_start, method='nearest', tolerance=self.NEAREST_POINT_TOLERANCE)]
            #    df_aggregate_at_end = self.dal.df.iloc[self.dal.df.index.get_loc(itr_time_interval_end, method='nearest', tolerance=self.NEAREST_POINT_TOLERANCE)]
            #    rdg_val_start = df_aggregate_at_start[self.AGGREGATION_COLUMN]
            #    rdg_val_end = df_aggregate_at_end[self.AGGREGATION_COLUMN]
            #except KeyError:
            #    pass
            #except IndexError:
            #    pass
            
            #rdg_val_start = 0
            #rdg_val_end = 0
            #If points are missing, we have to skip them.
            #if rdg_val_start == None or rdg_val_end == None: continue
            
            
            rdg_val_start, rdg_val_end, point_exists = self._getDataAtPoint(midpoint, itr_time_interval_start, itr_time_interval_end, self.time_interval)
            if not point_exists: continue
            
            pDiff = rdg_val_end - rdg_val_start
            
            pointX = midpoint
            pointY = pDiff
            #pointY = round(pDiff, config.ROUND_DECIMAL_PTS)
            pointY2_Start = rdg_val_start
            pointY2_End = rdg_val_end
            pointS = config.STATE_NORMAL
            
            subIterMin = itr_time_interval_start
            subIterMax = itr_time_interval_end
            
            if lowestTime == None: lowestTime = subIterMin
            else: lowestTime = min(lowestTime, subIterMin)
            
            if highestTime == None: highestTime = subIterMax
            else: highestTime = max(highestTime, subIterMax)
            
            
            #Don't let slight round errors pass
            min_val_interval, max_val_interval = (0, 0)
            if min_val_interval != None:
                min_val_interval = float(min_val_interval)
                min_val_interval = min(min_val_interval, pointY)
            if max_val_interval != None:
                max_val_interval = float(max_val_interval)
                max_val_interval = max(max_val_interval, pointY)
            
            if self.dal.metadata["thresholds"] != None:
                if not utils.isWithinBoundary(pointY, self.threshold["lower"], self.threshold["upper"]):
                    pointS = config.STATE_OOR
            
            p = {
                "x": pointX,
                "y": pointY,
                "y2": 0.0,
                "y2_start": pointY2_Start,
                "y2_end": pointY2_End,
                "state": pointS,
                "minmax": {
                    "min": min_val_interval,
                    "max": max_val_interval
                },
                "subTime": {
                    "min": subIterMin.isoformat() if timestampstr else subIterMin,
                    "max": subIterMax.isoformat() if timestampstr else subIterMax
                }
            }
            
            dataPoints.append(p)
        
        #Re-adjust start and end time (if changed)
        if lowestTime != None: self.dal.fromtime = utils.dt_local(lowestTime)
        if highestTime != None: self.dal.totime = utils.dt_local(highestTime)
        
        return dataPoints

    def _getReportColumns(self):
        return ["Rd.No","Start time","End time","Consumption (KWh)","Cumulative"], ["i", "dt", "dt", "f", "f"]

    #Chart title to reflect energy consumtion
    def _getChartTitle(self, data):
        return self._formatChartTitle(config.CHART_TITLE_ENERGY_CONSUMPTION, data)
    
    def _getChartSubplotConfig(self, data):
        return {
            'rows': 2,
            'cols': 1,
            'titles': (
                self._formatChartTitle(config.CHART_TITLE_ENERGY_CONSUMPTION, data),
                self._formatChartTitle(config.CHART_TITLE_ENERGY_CUMULATIVE, data)
            )
        }
    
    #Override to allow multiple charts
    def _makePlotlyFigure(self, data):
        subplot_cfg = self._getChartSubplotConfig(data)
        
        fig = make_subplots(
            rows=subplot_cfg['rows'],
            cols=subplot_cfg['cols'],
            subplot_titles=subplot_cfg['titles']
        )
        
        fig.update_layout(
            width = config.CHART_PLOTLY_WIDTH,
            height = config.CHART_PLOTLY_HEIGHT,
            autosize=False
        )
        
        return fig
    
    #Override the plotly chart
    def _generateImageChart(self, data):
        #Draw the regular Consumption chart
        fig = super()._generateImageChart(data)
        
        label = self._getYAxisLabel(data)
        
        #Subplot 2 layout
        fig.update_layout(
            xaxis2_linecolor='black',
            yaxis2_linecolor='black',
            
            xaxis2_mirror=True,
            yaxis2_mirror=True,
            
            xaxis2_showgrid=True,
            yaxis2_showgrid=True,
            
            xaxis2_showline=True,
            yaxis2_showline=True,
            
            xaxis2_title = 'Time',
            yaxis2_title = label,
            
            xaxis2_tickformat = config.CHART_XAXIS_FORMAT_PLOTLY
        )
        
        
        #Cumulative points
        x_points, y_points, x_trend, y_trend = [], [], [], []
        pY = None
        dY = 0
        meanDY = 0
        cnt = 0
        for x in data['dataset']:
            if pY == None:
                pY = x['y2']
            else:
                dY = x['y2'] - pY
                pY = x['y2']
                meanDY += dY
                cnt += 1
            x_points.append(utils.dt_local(x['x']))
            y_points.append(x['y2'])
        
        if len(x_points)>0 and len(y_points)>0:
            if cnt > 0:
                meanDY /= cnt
            
            x_trend.extend([x_points[0], x_points[-1]])
            y_trend.extend([y_points[0], len(y_points) * meanDY if meanDY != 0 else y_points[-1]])
        
        #Trendline
        fig.add_trace(go.Scatter(
            x=x_trend,
            y=y_trend,
            
            name=self._formatChartTitle(config.CHART_TRENDLINE_SERIES_NAME, data),
            line_shape="spline",
            line_width=config.CHART_TRENDLINE_THICKNESS,
            line_dash=config.CHART_TRENDLINE_TYPE[1],
            line_color=config.CHART_TRENDLINE_COLOR,
            mode="lines"
        ), row=2, col=1)
        
        #Add columns chart
        fig.add_trace(go.Bar(
            x=x_points,
            y=y_points,
            text=y_points,
            textposition="outside",
            #marker_color='rgb(26, 118, 255)',
            marker_color=config.CHART_LINE_COLOR,
            name=data['label']
        ), row=2, col=1)
        
        return fig
    
    
    #Override report table
    def _generateReportTable(self, time_interval):
        """
        Energy meter has two kinds of aggregates
        """
        
        data_raw = self.aggregate(time_interval, timestampstr=False)
        
        r_cols, r_dtypes = self._getReportColumns()
        
        #columns = ["Rd.No","Start time","End time","Consumption (KWh)","Cumulative (KWh)"]
        #columns = ["Rd.No","Start time","End time","EM-Consumption(KWh) / DG-Runtime(hrs) / Solar-Generation(KWh)","Cumulative"]
        #column_types = ["i", "dt", "dt", "f", "f"]
        
        metricValues = []
        states = []
        min_val, max_val = None, None
        for i, data in enumerate(data_raw['dataset']):
            start_time = data['subTime']['min']
            end_time = data['subTime']['max']
            
            val_y = data['y']
            val_y_cumu = data['y2']
            min_val = val_y if min_val is None else min(min_val, val_y)
            max_val = val_y if max_val is None else max(max_val, val_y)
            
            #Since this is for a report, we convert to localtime from UTC
            #Also excel hates the fact that a date contains a timezone, so we strip that away
            row_data = [
                i + 1,
                utils.dt_local(start_time).tz_localize(None),
                utils.dt_local(end_time).tz_localize(None),
                val_y,
                val_y_cumu
            ]
            metricValues.append(row_data)
            states.append(data['state'])
        
        return data_raw, r_cols, metricValues, states, {'minval': min_val, 'maxval': max_val, 'dtypes': r_dtypes}

    #Override function
    def _generateExcelChart(self, workbook, worksheet,
        data_raw,
        DataPoint_Styles,
        options,
        limits,
        table_pos,
        time_interval):
        
        #Create a regular chart
        super()._generateExcelChart(workbook, worksheet,
            data_raw,
            DataPoint_Styles,
            options,
            limits,
            table_pos,
            time_interval)
        
        #Add another chart
        chart1 = workbook.add_chart({'type': 'column'})
        
        category_column = 1
        value_column = 4
        
        cat_time_points = [utils.excel_date(utils.dt_local(x['x']).tz_localize(None)) for x in data_raw['dataset']]
        interval_days = utils.td_get_total_seconds(data_raw['interval']) / config.SECONDS_IN_DAY    #86400 Seconds in a day
        category_min = utils.excel_date(utils.dt_local(data_raw['from']).tz_localize(None))
        category_max = utils.excel_date(utils.dt_local(data_raw['to']).tz_localize(None))
        cat_time_points = [str(x) for x in cat_time_points]
        category_formula = "={" + ','.join(cat_time_points) + "}"
        #category_formula = ['Sheet1', table_pos[1], table_pos[0] + category_column, table_pos[2], table_pos[0] + category_column]
        
        chart1.add_series({
            'name': data_raw['label'],
            'values': ['Sheet1', table_pos[1], table_pos[0] + value_column, table_pos[2], table_pos[0] + value_column],
            'categories': category_formula,
            'marker': {'type': 'none'},
            'data_labels': {'value': True, 'position': 'outside_end'},
            'fill': {'color': config.CHART_LINE_COLOR},
            'trendline': {
                'name': self._formatChartTitle(config.CHART_TRENDLINE_SERIES_NAME, data_raw),
                'type': 'polynomial',
                'order': 2,
                'line': {
                    'color': config.CHART_TRENDLINE_COLOR,
                    'width': config.CHART_TRENDLINE_THICKNESS,
                    'dash_type': config.CHART_TRENDLINE_TYPE[0]
                },
            }
        })
        
        #Chart setup
        label = self._getYAxisLabel(data_raw)
        chart1.set_title({'name': self._formatChartTitle(config.CHART_TITLE_ENERGY_CUMULATIVE, data_raw)})
        chart1.set_x_axis({
            'name': 'Time', 
            'text_axis': True,          #Text-axis instead of date-axis due to Excel not showing date-time axis correctly.
                                        #See https://www.extendoffice.com/documents/excel/4203-excel-chart-with-date-and-time-on-x-axis.html
            'num_format': config.CHART_TICKS_TIME_FORMAT,
            'num_font': {'rotation': -45},
            'min': category_min,
            'max': category_max
        })
        chart1.set_y_axis({'name': label, 'min': 0})
        chart1.set_size({'x_scale': config.CHART_SCALE[0], 'y_scale': config.CHART_SCALE[1]})
        chart1.set_legend({'position': 'bottom'})
        
        
        #This time, add it below the added chart.
        #A chart is ~17 cells tall at 1x height scale
        c2_pos = table_pos[2] + round(17*config.CHART_SCALE[1])
        worksheet.insert_chart(c2_pos, 0, chart1)


#Metrics that require finding an earlier point than selected in order to calculate a cumulative sum. Eg: DG Run Hours, Solar Total Energy
class CumulativeHistoric(Cumulative):
    def _getDataAtPoint(self, midpoint, itr_time_interval_start, itr_time_interval_end, time_interval):
        #print('[Interval start NOW123]', flush=True)
        #print("Trying to find a pair of points for", midpoint, ', in interval [', itr_time_interval_start ,',', itr_time_interval_end, ']', flush=True)
        rdg_val_start, rdg_val_end, pointFound = super()._getDataAtPoint(midpoint, itr_time_interval_start, itr_time_interval_end, time_interval)
        
        if not pointFound:
            #print('[Not found using original tolerance, searching within whole interval]', flush=True)
            rdg_val_start, rdg_val_end = None, None
            p1Found = False
            p2Found = False
            
            df_intv = self.dal.df.iloc[(self.dal.df.index >= itr_time_interval_start) & (self.dal.df.index <= itr_time_interval_end)]
            
            try:
                #df_aggregate_at_start = self.dal.df.iloc[self.dal.df.index.get_loc(itr_time_interval_start, method='pad')]
                df_aggregate_at_start = df_intv.iloc[df_intv.index.get_loc(itr_time_interval_start, method='pad')]
                rdg_val_start = df_aggregate_at_start[self.AGGREGATION_COLUMN]
                #print("Found start point at", df_aggregate_at_start.name, ', value:', rdg_val_start, ', Actual:', itr_time_interval_start, flush=True)
                p1Found = True
            except KeyError:
                pass
            except IndexError:
                pass
            
            try:
                #df_aggregate_at_end = self.dal.df.iloc[self.dal.df.index.get_loc(itr_time_interval_end, method='backfill')]
                df_aggregate_at_end = df_intv.iloc[df_intv.index.get_loc(itr_time_interval_end, method='backfill')]
                rdg_val_end = df_aggregate_at_end[self.AGGREGATION_COLUMN]
                #print("Found end point at", df_aggregate_at_end.name, ', value:', rdg_val_end, ', Actual:', itr_time_interval_end, flush=True)
                p2Found = True
            except KeyError:
                pass
            except IndexError:
                pass
            
            
            pointFound = p1Found and p2Found
            
            #if not p1Found:
            #    print('[Start point not found, going ultra mode]', flush=True)
            #if not p2Found:
            #    print('[End point not found, going ultra mode]', flush=True)
            
        if not pointFound:
            #print("[Ultra mode]", flush=True)
            rdg_val_start, rdg_val_end = None, None
            pointFound = False
            
            try:
                df_aggregate_at_start = self.dal.df.iloc[self.dal.df.index.get_loc(itr_time_interval_start, method='backfill')]
                rdg_val_start = df_aggregate_at_start[self.AGGREGATION_COLUMN]
                rdg_val_end = rdg_val_start
                pointFound = True
            except KeyError:
                pass
            except IndexError:
                pass
        
        #if not pointFound:
        #    print("[Ultra mode failed]", flush=True)
        
        #print('[Done]', flush=True)
        return rdg_val_start, rdg_val_end, pointFound


#==============================
# Endpoint classes for metrics
#==============================

class Temperature(Timeseries):
    pass

class Humidity(Timeseries):
    def __init__(self, dal):
        super().__init__(dal)
        self.dal.COLUMN_METRIC_RDG_RAW_VALUE = "rh_value"
        self.dal.COLUMN_METRIC_RDG_VALUE = "rh_avg_15min"
        self.dal.COLUMN_METRIC_RDG_VALUE_30M = "rh_avg_30min"
        self.dal.COLUMN_METRIC_RDG_VALUE_60M = "rh_avg_60min"
        self.dal.COLUMN_METRIC_RDG_VALUE_24H = "rh_avg_24h"
        self.dal.COLUMN_METRIC_RDG_VALUE_MTD = "rh_avg_mtd"
        
        self.dal.COLUMN_METRIC_NONAGG_MIN = "rh_min_15minutes"
        self.dal.COLUMN_METRIC_NONAGG_MAX = "rh_max_15minutes"
        self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_VALUE

class EnergyMeter_PF(Timeseries):
    def __init__(self, dal):
        super().__init__(dal)
        self.dal.COLUMN_METRIC_RDG_RAW_VALUE = "pf_value"
        self.dal.COLUMN_METRIC_RDG_VALUE = "pf_avg_15min"
        self.dal.COLUMN_METRIC_RDG_VALUE_30M = "pf_avg_30min"
        self.dal.COLUMN_METRIC_RDG_VALUE_60M = "pf_avg_60min"
        self.dal.COLUMN_METRIC_RDG_VALUE_24H = "pf_avg_24h"
        self.dal.COLUMN_METRIC_RDG_VALUE_MTD = "pf_avg_mtd"
        self.dal.COLUMN_METRIC_NONAGG_MIN = "pf_min_15minutes"
        self.dal.COLUMN_METRIC_NONAGG_MAX = "pf_max_15minutes"
        self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_VALUE

class EnergyMeter(Cumulative):
    def __init__(self, dal):
        super().__init__(dal)
        
        self.dal.COLUMN_METRIC_RDG_RAW_VALUE = "kwh_value"
        self.dal.COLUMN_METRIC_RDG_VALUE = "kwh_agg_15min"
        self.dal.COLUMN_METRIC_RDG_VALUE_30M = "kwh_agg_30min"
        self.dal.COLUMN_METRIC_RDG_VALUE_60M = "kwh_agg_60min"
        self.dal.COLUMN_METRIC_RDG_VALUE_24H = "kwh_agg_24h"
        self.dal.COLUMN_METRIC_RDG_VALUE_MTD = "kwh_agg_mtd"
        self.dal.COLUMN_METRIC_NONAGG_MIN = "kwh_min_15minutes"
        self.dal.COLUMN_METRIC_NONAGG_MAX = "kwh_max_15minutes"
        self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_RAW_VALUE

    def _getReportColumns(self):
        return ["Rd.No","Start time","End time","EM-Consumption (KWh)","Cumulative"], ["i", "dt", "dt", "f", "f"]

class DGRunHours(CumulativeHistoric):
    def __init__(self, dal):
        super().__init__(dal)
        
        self.dal.COLUMN_METRIC_RDG_RAW_VALUE = "dg_value"
        self.dal.COLUMN_METRIC_RDG_VALUE = "dg_agg_15min"
        self.dal.COLUMN_METRIC_RDG_VALUE_30M = "dg_agg_30min"
        self.dal.COLUMN_METRIC_RDG_VALUE_60M = "dg_agg_60min"
        self.dal.COLUMN_METRIC_RDG_VALUE_24H = "dg_agg_24h"
        self.dal.COLUMN_METRIC_RDG_VALUE_MTD = "dg_agg_mtd"
        self.dal.COLUMN_METRIC_NONAGG_MIN = "dg_min_15minutes"
        self.dal.COLUMN_METRIC_NONAGG_MAX = "dg_max_15minutes"
        self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_RAW_VALUE
    
    def _getReportColumns(self):
        return ["Rd.No","Start time","End time","DG-Runtime (Hrs)","Cumulative"], ["i", "dt", "dt", "f", "f"]


class SolarEMTotal(CumulativeHistoric):
    def __init__(self, dal):
        super().__init__(dal)
        
        self.dal.COLUMN_METRIC_RDG_RAW_VALUE    = "solar_value"
        self.dal.COLUMN_METRIC_RDG_VALUE        = "solar_agg_15min"
        self.dal.COLUMN_METRIC_RDG_VALUE_30M    = "solar_agg_30min"
        self.dal.COLUMN_METRIC_RDG_VALUE_60M    = "solar_agg_60min"
        self.dal.COLUMN_METRIC_RDG_VALUE_24H    = "solar_agg_24h"
        self.dal.COLUMN_METRIC_RDG_VALUE_MTD    = "solar_agg_mtd"
        self.dal.COLUMN_METRIC_NONAGG_MIN       = "solar_min_15minutes"
        self.dal.COLUMN_METRIC_NONAGG_MAX       = "solar_max_15minutes"
        self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_RAW_VALUE

    def _getReportColumns(self):
        return ["Rd.No","Start time","End time","Solar-Generation (KWh)","Cumulative"], ["i", "dt", "dt", "f", "f"]
