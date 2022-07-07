from fastapi import FastAPI 
import uvicorn

from module import json_to_csv as csv
import data_access as db

app=FastAPI()

@app.post('/post_data')
def json_listener(data_ :dict):
    for row in data_["sensor_value"]:
        db.insert_data_into_history_table(row)

@app.get('/sensor_master')
def get_sensor_data():
    return db.get_data_from_sensor_master()[1]

@app.get('/history_data')
def get_history_data():
    return db.get_history_data()[1]

if __name__ == '__main__':
    uvicorn.run(app, host="localhost", port=5000)