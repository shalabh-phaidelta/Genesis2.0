
"""
Aggregator generic class that contains logic commonly used by metric charting classes
"""

#Standard Libraries
from datetime import timedelta, timezone, time
from io import BytesIO
from base64 import b64encode

#Common libraries
from ..common import config
from ..common import utils


#===Dependencies===

#Numpy
import numpy as np

#Pandas
import pandas as pd

#PDF Output
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle

from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.units import inch, cm
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

#Excel xlsx output
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_cell_to_rowcol


class Aggregator:
    """
    Aggregator class that is responsible for generating metric chart data
    and reports on the server
    """
    
    dal = None
    
    #Aggregation interval that is being used to calculate further intervals
    AGGREGATE_DB_INTERVAL = timedelta(minutes=15)
    THRESHOLD_FOR_INTERVAL = timedelta(minutes=15)
    NEAREST_POINT_TOLERANCE = timedelta(minutes=3)      #TODO: Make it back to 1 minute once loader is fixed
    AGGREGATION_COLUMN = None
    TIME_COLUMNS = []
    
    ADD_INTERACTIVE_CHART = False
    ADD_CHART_REPORT = True
    ExcelConditionFormat = True
    
    def __init__(self, dal):
        self.dal = dal
        self.AGGREGATION_COLUMN = self.dal.COLUMN_METRIC_RDG_VALUE
        for col in self.TIME_COLUMNS:
            self.dal.df[col] = pd.to_datetime(self.dal.df[col]).apply(utils.inputTimeSanitize)
    
    
    #==== Metric chart JSON related ====
    
    def _calculateSumBetweenFrame(self, itr_time_interval_start, itr_time_interval_end, time_interval):
        """
        Aggregate data between a timeframe and with time interval
        """
        
        total, tCount = 0.0, 0
        min_value, max_value = None, None
        got_aggretate_points = []
        aggregate_count = time_interval // self.AGGREGATE_DB_INTERVAL
        min_value_list, max_value_list = [], []
        
        for temp in range(0, aggregate_count):
            displacement = temp * self.AGGREGATE_DB_INTERVAL
            ptime = itr_time_interval_end - displacement
            
            if (ptime-self.AGGREGATE_DB_INTERVAL < itr_time_interval_start): continue
            
            try:
                df_aggregate_at = self.dal.df.iloc[self.dal.df.index.get_loc(ptime, method='nearest', tolerance=self.NEAREST_POINT_TOLERANCE)]
                rdg_val = df_aggregate_at[self.AGGREGATION_COLUMN]
                min_val = df_aggregate_at[self.dal.COLUMN_METRIC_NONAGG_MIN]
                max_val = df_aggregate_at[self.dal.COLUMN_METRIC_NONAGG_MAX]
                
                min_value_list.append(min_val)
                max_value_list.append(max_val)
                
                total += float(rdg_val)
                
                #Initial value
                #if pd.isnull(min_value) == None: min_value = min_val
                #if pd.isnull(max_value) == None: max_value = max_val
                '''
                if not pd.isnull(min_val):
                    new_min = min(min_value, min_val)
                    if not pd.isnull(new_min):
                        min_value = new_min
                    #min(min_val if pd.isnull(min_value) else min_value, min_val)
                if not pd.isnull(max_val):
                    new_max = min(min_value, min_val)
                    if not pd.isnull(new_min):
                        min_value = new_min
                    max_value = max(max_val if pd.isnull(max_value) else max_value, max_val)
                '''
                
                got_aggretate_points.append((ptime, rdg_val))
                
                tCount += 1
            except KeyError:
                pass
            except IndexError:
                pass
        
        for i in min_value_list:
            if not pd.isnull(i):
                min_value = min(i if pd.isnull(min_value) else min_value, i)
        
        for i in max_value_list:
            if not pd.isnull(i):
                max_value = max(i if pd.isnull(max_value) else max_value, i)
        
        if pd.isnull(min_value): min_value = None
        if pd.isnull(max_value): max_value = None
        
        return total, tCount, (min_value, max_value), got_aggretate_points
    
    
    #== Public Functions / Interface functions ==
    
    #Functions that will be overridden
    def aggregate(self, time_interval, alternateFormat=False, timestampstr=True):
        pass
    
    def latest(self, time_interval):
        pass
    
    def _generateReportTable(self, time_interval):
        pass
    
    #==== Report generation functions ====
    
    #Each metric constructs it's own chart
    def _generateImageChart(self, data):
        pass
    
    def _formatChartTitle(self, title_format_string, data):
        """
        Generic string format, but using pre-defined format identifiers
        """
        
        return title_format_string.format(
            s_name=data['metric_name'],
            t_interval=utils.td2human(data['interval']) if 'interval' in data.keys() else '',
            t_start=utils.dt_local(data['from']).strftime(config.HUMAN_TIME_FORMAT),
            t_end=utils.dt_local(data['to']).strftime(config.HUMAN_TIME_FORMAT)
        )
    
    def _getReportTitle(self, data):
        """
        Normal title for report
        """
        return self._formatChartTitle(config.REPORT_TITLE_GENERAL, data)
    
    def _getChartTitle(self, data):
        """
        Normal title for a chart
        """
        return self._formatChartTitle(config.CHART_TITLE_GENERAL, data)
    
    
    def _getYAxisLabel(self, data):
        """
        Y axis label for charts.
        """
        
        label = ''
        if data['measure_unit'] is not None:
            if data['measure_unit'][1] is not None:
                label += data['measure_unit'][1]
                if data['measure_unit'][0] != '' and data['measure_unit'][0] is not None:
                    label += ' ({})'.format(data['measure_unit'][0])
        
        return label
    
    def generateImage(self, time_interval=None, data=None):
        """
        Function to render an image in memory
        if data is passed directly, other parameters are ignored.
        """
        
        #In-memory Image
        img = BytesIO()
        
        #Get data
        if data is None:
            data = self.aggregate(time_interval, False, False)
        
        #Render the chart using metric data
        fig = self._generateImageChart(data)
        
        #Render image using Kaleido
        fig.write_image(img, engine="kaleido")
        
        fig_html = fig.to_html(include_plotlyjs='cdn')
        
        #Go back to the beginning
        img.seek(0)
        
        return img, fig_html
    
    def _makeReportHeading(self, data):
        """
        Return a table with the heading table of the report
        """
        
        mtr_id = utils.metric_id_to_dict(data['label'])
        MTR_LOC = config.METRIC_LOCATION_FULLNAME[mtr_id['Region'].lower()]
        MTR_WHS = mtr_id['Warehouse']
        MTR_BDG = mtr_id['Building']
        MTR_UNT = mtr_id['Floor'] + ' ' + mtr_id['Unit']
        MTR_UTL = config.STRING_NA
        MTR_LTL = config.STRING_NA
        MTR_LCL = config.STRING_NA
        MTR_UCL = config.STRING_NA
        MTR_INT = config.STRING_NA
        MTR_ALIAS = data['metric_name']
        
        if 'interval' in data.keys():
            MTR_INT = utils.td2human(data['interval'])
        
        if 'control_limits' in data:
            if data['control_limits']['LCL'] != None: MTR_LCL = data['control_limits']['LCL']
            if data['control_limits']['UCL'] != None: MTR_UCL = data['control_limits']['UCL']
        if 'threshold' in data:
            if data["threshold"] is not None:
                if data["threshold"]["lower"] is not None:
                    MTR_LTL = float(data["threshold"]["lower"])
                if data["threshold"]["upper"] is not None:
                    MTR_UTL = float(data["threshold"]["upper"])
        
        MTR_TYPE = self.dal.metadata['metric_type']
        report_table = [["Location", MTR_LOC, "Warehouse", MTR_WHS, "Report Interval", MTR_INT],
                ["Building", MTR_BDG, "Unit", MTR_UNT, "Metric Type", MTR_TYPE],
                ["Lower Limit", MTR_LTL, "Upper Limit", MTR_UTL, "Sensor Name", MTR_ALIAS]]
        
        lmt = (MTR_UCL, MTR_LCL, MTR_UTL, MTR_LTL)
        
        return report_table, lmt
    
    
    def generatePDF(self, time_interval):
        """
        Generate a PDF report from the metric data and
        returns the PDF file as a binary array (in-memory file)
        """
        f_pdf = BytesIO()
        
        doc  = SimpleDocTemplate(f_pdf, pagesize=A4,
                        rightMargin=72,leftMargin=72,
                        topMargin=72,bottomMargin=18)
        styles=getSampleStyleSheet()
        
        #PDF template builder list
        pdfTempl = []
        
        #==========
        #Start building PDF
        #==========
        
        #Load data (note, this function is implemented in descendent classes)
        data_raw, tbl_hdr, tbl_rows, tbl_states, data_options = self._generateReportTable(time_interval)
        
        #Load metric information
        report_table, limits = self._makeReportHeading(data_raw)
        report_title = self._getReportTitle(data_raw)
        
        #Set doc metadata
        doc.title = report_title
        doc.author = "Genesis"
        
        #Heading
        pdfTempl.append(Paragraph(report_title, styles["title"]))
        
        pdfTempl.append(Spacer(-1, 1*cm))
        
        #Styles
        MetaNormal = ParagraphStyle('MetaNormal', parent=styles['Normal'], alignment=TA_CENTER)
        MetaBold = ParagraphStyle('MetaBold', parent=MetaNormal, fontWeight="BOLD")
        MetaRed = ParagraphStyle('MetaRed', parent=MetaBold, textColor=colors.red)
        LinkRight = ParagraphStyle('NormalRight', parent=styles['Normal'], alignment=TA_RIGHT, fontWeight="BOLD")
        LinkRightBlue = ParagraphStyle('NormalRight', parent=styles['Normal'], alignment=TA_RIGHT, textColor=colors.blue)
        
        #Report Details
        metaTableData = []
        for row, r_data in enumerate(report_table):
            mRow = []
            for col, c_data in enumerate(r_data):
                style = MetaBold if col % 2 == 0 else MetaNormal
                mRow.append(Paragraph(str(c_data), style))
            metaTableData.append(mRow)
        
        metadataTable = Table(metaTableData, colWidths=3*cm, rowHeights=1.5*cm)
        metadataTable.setStyle(TableStyle([
            ('BACKGROUND',(1,0),(1,-1), '#cccccc'),
            ('BACKGROUND',(3,0),(3,-1), '#cccccc'),
            ('BACKGROUND',(5,0),(5,-1), '#cccccc'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 0.25, colors.black),
        ]))
        pdfTempl.append(metadataTable)
        
        pdfTempl.append(Spacer(-1, 1.5*cm))
        
        #Simple date format handler
        def dt_to_str(x):
            if type(x) is pd.Timestamp:
                return x.strftime(config.HUMAN_TIME_FORMAT)
            elif type(x) is time:
                return x.strftime('%H:%M')
            return str(x)
        
        #Data table
        metricData=[]
        
        #Add table headings
        metricData.append([Paragraph(t, MetaBold) for t in tbl_hdr])
        
        #Format the data to be suitable for reportlab
        for row, data in enumerate(tbl_rows):
            metricData.append([
                Paragraph(dt_to_str(t), MetaNormal if tbl_states[row] is config.STATE_NORMAL else MetaRed)
                for t in data
            ])
        
        COLUMN_WIDTH_FOR_DTYPE = {
            'default': None,
            'i': 1.5 * cm,
            'dt': 3.7*cm,
            's': None
        }
        
        try:
            #Note: 3.7cm fits the date timestamp with some headroom
            report_data_column_widths = [
                COLUMN_WIDTH_FOR_DTYPE[dtype] if dtype in COLUMN_WIDTH_FOR_DTYPE.keys() else COLUMN_WIDTH_FOR_DTYPE['default']
                for dtype in data_options['dtypes']
            ]
        except TypeError:
            #Default width
            report_data_column_widths = 3.7*cm
        
        
        #Create a table and dump the data
        metricDataTable = Table(metricData, colWidths=report_data_column_widths)
        
        #Make the table fancy
        metricDataTable.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0), '#cccccc'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 0.25, colors.black),
        ]))
        pdfTempl.append(metricDataTable)
        
        
        pdfTempl.append(Spacer(-1, 1.5*cm))
        
        
        #Make image with same parameters
        if self.ADD_CHART_REPORT:
            chart_table_elements = []
            chart_html_url = "data:text/html;base64,"
            chartAdded = False
            
            try:
                chart_img, chart_html = self.generateImage(data=data_raw)
                chart_height = config.CHART_PDF_WIDTH_CM * config.CHART_PLOTLY_HEIGHT / config.CHART_PLOTLY_WIDTH
                chart_elem = Image(chart_img, config.CHART_PDF_WIDTH_CM*cm, chart_height*cm)
                
                #We need to encapsulate the image within a table
                #as reportlab has no easy way of adding a border to images
                chart_table_elements.append(chart_elem)
                
                chart_html_url += b64encode(chart_html.encode()).decode()
                
                chartAdded = True
            except OSError:
                chartAdded = False
            except ValueError:
                chartAdded = False
            
            if not chartAdded:
                #Failed to render the image, bail out
                chart_table_elements.append(Paragraph("Chart not available", MetaNormal))
            elif self.ADD_INTERACTIVE_CHART:
                #Interactive Chart link
                pdfTempl.append(Spacer(-1, 1 * cm))
                interactive_chart_link = '<a target="_blank" href="' + chart_html_url + '"><u>' + 'View interactive chart' + '</u></a>'
                pdfTempl.append(Paragraph(interactive_chart_link, LinkRightBlue))
                pdfTempl.append(Spacer(-1, 0.3*cm))
            
            chartTable = Table(data=[chart_table_elements])
            chartTable.setStyle(TableStyle([
                ('BOX', (0, 0), (-1, -1), 0.25, colors.black),
            ]))
            
            pdfTempl.append(chartTable)
        
            
        #Homepage
        pd_home_link = '<a target="_blank" href="' + 'http://phaidelta.com' + '"><u>' + 'phAIdelta.com' + '</u></a>'
        pdfTempl.append(Paragraph(pd_home_link, LinkRight))
        
        #Done. Now render the entire document
        doc.build(pdfTempl)
        
        #The pdf has been written to the "file", so we rewind
        f_pdf.seek(0)
        
        return f_pdf
    
    
    def generateXLSX(self, time_interval):
        """
        Generate an XLSX (Excel) report from the metric data and
        returns the XLSX file as a binary array (in-memory file)
        """
        
        #Set to True to make the table have filters. And Bands.
        USE_TABLE = False
        
        #Width of most of the clumns containing data
        BASE_COLUMN_WIDTH = 18
        
        #In-memory XLSX
        xlsx = BytesIO()
        
        #Create a new workbook with one sheet
        workbook = xlsxwriter.Workbook(xlsx, {'default_date_format': config.HUMAN_EXCEL_DATE_FORMAT})
        worksheet = workbook.add_worksheet()
        
        #Hide gridlines
        worksheet.hide_gridlines(2)
        
        #Edit protect
        if config.EXCEL_WRITE_PROTECT:
            worksheet.protect()
        
        #Add some styles
        format_param = workbook.add_format({'bg_color': '#cccccc', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
        format_hdg_black = workbook.add_format({'font_color': 'black', 'font_size': 18})
        format_hdg_red = workbook.add_format({'font_color': 'red', 'font_size': 18})
        format_tbl_hdg_bold = workbook.add_format({'bg_color': '#e8e8e8', 'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'border': 1})
        format_datetime = workbook.add_format({'num_format': config.HUMAN_EXCEL_DATE_FORMAT, 'align': 'center', 'border': 1})
        format_rdg_normal = workbook.add_format({'font_color': 'black', 'align': 'center', 'border': 1})
        format_rdg_oor = workbook.add_format({'font_color': 'red', 'bold': True, 'align': 'center'})
        
        #Load data
        data_raw, tbl_hdr, tbl_rows, tbl_states, data_options = self._generateReportTable(time_interval)
        
        report_table, limits = self._makeReportHeading(data_raw)
        
        #Heading
        #the * will unpack the tuple into arguments
        worksheet.write(
            'A1',
            self._getReportTitle(data_raw),
            format_hdg_black
        )
        
        #Metric Information
        metric_data_pos_row, metric_data_pos_col = xl_cell_to_rowcol('B3')
        
        #Unused, will be used to format the row colors
        badcells=[str(r+1) for r, s in enumerate(tbl_states) if s != config.STATE_NORMAL]
        
        for row, r_data in enumerate(report_table):
            for col, c_data in enumerate(r_data):
                style = format_tbl_hdg_bold if col % 2 == 0 else format_param
                if type(c_data) is int or type(c_data) is float:
                    worksheet.write(metric_data_pos_row+row, metric_data_pos_col+col, c_data, style)
                else:
                    worksheet.write(metric_data_pos_row+row, metric_data_pos_col+col, str(c_data), style)
                worksheet.set_column(metric_data_pos_col+col, metric_data_pos_col+col, BASE_COLUMN_WIDTH)
            worksheet.set_row(metric_data_pos_row+row, 48)
        
        #==============================
        #From here is Metric-dependent
        #==============================
        #Table
        TABLE_CELL = "A8"
        DataPoint_Styles = []
        min_value = 0
        
        #A8 in excel in terms of numbers
        table_position = xl_cell_to_rowcol(TABLE_CELL)
        table_posY, table_posX = table_position
        
        #Data will start from the next row, A9.
        #This will keep track of how large the table is
        row_pos = table_posY + 1
        
        #Write data first
        for row, data in enumerate(tbl_rows):
            for col, cell in enumerate(data):
                style = format_rdg_normal
                
                if 'dtypes' in data_options:
                    if data_options['dtypes'][col] == 'dt':
                        try:
                            worksheet.write_datetime(table_posY+row+1, table_posX+col, cell, format_datetime)
                        except TypeError:
                            worksheet.write(table_posY+row+1, table_posX+col, str(cell), style)
                    else:
                        worksheet.write(table_posY+row+1, table_posX+col, cell, style)
            row_pos+=1
        
        #Increase column width for consistency
        worksheet.set_column(table_posX, table_posX+7, BASE_COLUMN_WIDTH)
        
        #Conditional Formatting the table
        value_column_start = xl_rowcol_to_cell(table_posY+1, 3)
        
        #Kya formula hai yaar
        #outofthres_formula='=SUMPRODUCT(--(${}={{{}}}))>0'.format(value_column_start, ",".join(badcells))
        outofthres_formula='=OR(AND($C$5<>"{1}", ${0}<$C$5), AND($E$5<>"{1}", ${0}>$E$5))'.format(value_column_start, config.STRING_NA)
        
        if self.ExcelConditionFormat:
            #Highlight red those which are out of range
            worksheet.conditional_format(table_posY+1,table_posX,row_pos-1,table_posX+len(tbl_hdr)-1,{
                'type':     'formula',
                'criteria': outofthres_formula,
                'format':   format_rdg_oor
            })        
        
        if USE_TABLE:
            #Add an Excel Table with heading and Filter
            worksheet.add_table(table_posY, table_posX, row_pos-1, table_posX+len(tbl_hdr)-1,{
                'header_row': True,
                'columns': [{'header': x, "header_format": format_tbl_hdg_bold} for x in tbl_hdr]
            })
        else:
            #Just write down the headings normally
            worksheet.write_row(table_posY, table_posX, tbl_hdr, format_tbl_hdg_bold)
        
        
        #Chart
        if self.ADD_CHART_REPORT:
            #Construct Data point Colors list
            DataPoint_Styles = [
                {'fill': {'color': config.CHART_MARKER_COLOR_NORMAL if s == config.STATE_NORMAL else config.CHART_MARKER_COLOR_OOR}}
                for s in tbl_states
            ]
            
            #Create an Excel chart from child class
            self._generateExcelChart(
                workbook, worksheet, data_raw,
                DataPoint_Styles,
                data_options,
                limits,
                (table_posX, table_posY+1,row_pos-1),
                time_interval
            )
        
        #Done. This will write to the "file"
        workbook.close()
        
        #"It's rewind time"
        xlsx.seek(0)
        
        return xlsx

class ChartData:
    __fields = None
    def __init__(self):
        self.__fields = {}
        self.setDatetimeFormat()
    def parseDAL(self, dal):
        mtr_label = dal.metadata['metric_name']
        mtr_iswhlvl = int(dal.metadata['iswhlunit'])
        
        loc_name = dal.metadata['location_alias']
        if loc_name == None: loc_name = dal.metadata['location_name']
        unit_name = dal.metadata['unit_alias']
        if unit_name == None: unit_name = '_'.join(dal.metadata['unit_name'].split('_')[2:5])
        mtr_name = dal.metadata['metric_alias']
        if mtr_name == None: mtr_name = '_'.join(dal.metadata['metric_name'].split('_')[5:])
        
        self.__fields['from'] = dal.fromtime
        self.__fields['to'] = dal.totime
        self.__fields['metric_id'] = dal.metadata['metric_id']
        self.__fields['metric_name'] = mtr_name
        self.__fields['label'] = mtr_label
        self.__fields['location_id'] = dal.metadata['location_id']
        self.__fields['location_name'] = loc_name
        self.__fields['unit_id'] = dal.metadata['unit_id']
        self.__fields['unit_name'] = unit_name
        self.__fields[dal.COLUMN_METRIC_IS_WHL_MTR] = mtr_iswhlvl
    
    def __getParsedFields(self):
        r = {}
        for key in self.__fields.keys():
            if self.__dtFormat == 'iso' and (key == 'from' or key == 'to'):
                val = self.__fields[key].isoformat()
            else:
                val = self.__fields[key]
            r[key] = val
        return r
    
    def setDatetimeFormat(self, format='same'):
        """
        format:
        same - keep datetime fields as is
        iso - convert datetime fields to strings formatted with ISO8601
        """
        self.__dtFormat = format
    def setDataset(self, data):
        self.__fields['dataset'] = data
    def set(self, key, data):
        self.__fields[key] = data
    def toJSON(self):
        return self.__getParsedFields()
