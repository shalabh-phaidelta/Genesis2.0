import dash
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
from datetime import datetime, date , timedelta

import sqlite3

# con = sqlite3.connect("test.db")

# data = pd.read_csv("avocado.csv")


class dotdict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__



def get_connector():
    mydb = sqlite3.connect('genesis_db.db')
    return mydb

def read_data_from_history_table():
    df = pd.read_sql('SELECT * FROM history_table ht WHERE sensor_ts >0', get_connector())
    return df

def get_list_of_sensor():
    df = pd.read_sql('SELECT DISTINCT (sensor_name) FROM history_table ht', get_connector())
    return df['sensor_name']

def get_dropdown_from_df(df,label, value):
    option_list = []
    for index, row in df.iterrows():
        option_list.append({'label': row[label], 'value': row[value]})
    return option_list

def create_graph(df, x_, y_,title=''):
    global colors
    graph = html.Div(
            dcc.Graph(
            id = 'Output-graph',

            figure={

                "data": [

                    {

                        "x": df[x_], #[df[column_]==key]

                        "y": df[y_],#[df[column_]==key]

                        "type": "line",

                    },

                ],

                "layout": {
                    "title": title,
                    'paper_bgcolor': colors['background'],
                    'font': {
                        'color': colors['text']
                             }
                        },

            },

        ),
    )

    return graph
    pass

def data_filter(value, start_date, end_date, aggr):
    data = read_data_from_history_table()
    data['Date'] = pd.to_datetime(data['sensor_ts'], unit='s')
    data.sort_values("Date", inplace=True)
    dff = data.loc[data['sensor_name']==value]
    current_date_temp = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    end_date = current_date_temp.strftime("%Y-%m-%d")

    dff = dff[(dff['Date'] > start_date ) & (dff['Date'] < end_date )]
    return dff[['Date','sensor_value']].groupby(pd.Grouper(key='Date', axis=0, freq=aggr)).mean().reset_index().dropna()
    # return dff[['Date','sensor_value']].dropna()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

colors = {
    'background': '#000C47',
    'text': '#ECEBE5'
}

app.layout = html.Div( style={'backgroundColor': colors['background']},

    children=[

        html.H1(children="Site Sensor data",style={
            'textAlign': 'center',
            'color': colors['text']
        }),

        html.P(

            children="site data",
            style={
                'textAlign': 'left',
                'color': colors['text']
                }
        ),
        html.Div([

            dcc.Dropdown(list(get_list_of_sensor()), style={'color' : '#101010' , "width": "38rem"}, id ='sensor_selection', placeholder=list(get_list_of_sensor())[0]),
            # dcc.Dropdown(["15min", "1H","1D"], style={'color' : '#101010' , "width": "38rem"}, id ='aggregation_selection', placeholder="15min"),
            dcc.DatePickerRange(
                id='my-date-picker-range',
                min_date_allowed=date(1995, 8, 5),
                max_date_allowed=date.today(),
                initial_visible_month=date.today().replace(day=1),
                end_date=date.today()
            ),

    ],
                style={
                'textAlign': 'left',
                'color': '#101010'
                }
    ),

        html.Div(id='Selected-sensor', style={
        'textAlign': 'center',
        'color': colors['text']}),
    ]

)

@app.callback(
    dash.Output('Selected-sensor', 'children'),
    dash.Input('sensor_selection', 'value')    ,
    # dash.Input('aggregation_selection', 'aggr'),
    dash.Input('my-date-picker-range', 'start_date'),
    dash.Input('my-date-picker-range', 'end_date')

)
def update_output(value,start_date , end_date  ,aggr='15min'):
    if start_date is None:
        start_date = '2022-07-18'
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")
    if value is None:
        value=list(get_list_of_sensor())[0]
    df = data_filter(value, start_date, end_date, aggr)
    return create_graph(df,'Date', 'sensor_value', title=f"Ploating graph for {value} between {start_date} and {end_date}")


if __name__ == "__main__":
    app.run_server(debug=True)
