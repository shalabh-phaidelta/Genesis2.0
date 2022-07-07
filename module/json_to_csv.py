import csv
from datetime import datetime


def append_data_in_csv(data):
    with open("data.csv",'a' , newline='') as csv_file:
        writer_object = csv.DictWriter(csv_file,fieldnames=data['sensor_value'][0].keys())
        print(writer_object.fieldnames)
        for row in data['sensor_value']:
            try:
                writer_object.writerow(row)
            except ValueError :
                print("Faced Value error while adding ",row)

