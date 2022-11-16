import contextlib
from . import models as db , vw_table as vw
from sqlalchemy import  orm, exc

normality = [
    'NORMAL',
    'OUT_OF_RANGE',
    'INACTIVE'
]


def call_proc_result(session, proc_name, arg):
    print(f"calling Stroeproc {proc_name} with {session}  with para :  {arg}")
    pass

def strip_list(input_list):
    output_list = []
    for i in input_list:
        output_list.append(i[0])
    return output_list

def get_item_from_list(list_):
    data = []
    for item in list_:
        data.append(item[0])
    return data

def update_token_for_user(user_id : int, token : str):
    with db.create_sesion() as session:
        session.query(db.User_Master).filter(db.User_Master.user_id==user_id).update({db.User_Master.token : token}) 
        session.commit()  

# def stp_get_locations_state_for_user(user_id):
#     data = []
#     with db.create_sesion() as session:
#         try:

#             loc_list = session.query(db.User_location_unit_map.location_id).filter(db.User_location_unit_map.user_id == user_id).all()
#             print(loc_list)
#             location_details = session.query(db.Location_Master).filter(db.Location_Master.location_id.in_(get_item_from_list(loc_list)) ).all()
#             print(location_details)
#             for i in location_details:                
#                 data.append({
#                     'id': i.location_id,
#                     'name': i.global_location_name,
#                     'latitude': i.latitude,
#                     'longitude': i.longitude,
#                     'state' : 'Normal'
#                 })
#         except StopIteration:
#             raise ValueError("Failed to Fetch data. Procedure did not return any results.")
#     print(data)
#     return data

def warehouse_view_warehouse_level_metrics(user_id :int , location_id :int):
    with db.create_sesion() as session:
        try:
            return session.query(vw.VW_Metric_Summary_Latest_unit_Level).filter(
                vw.VW_Metric_Summary_Latest_unit_Level.user_id == user_id and 
                vw.VW_Metric_Summary_Latest_unit_Level.location_id==location_id and 
                vw.VW_Metric_Summary_Latest_unit_Level.is_warehouse_level_unit == True)
        except StopIteration:
            raise ValueError("Failed to fetch data. Procedure did not return any result")

def stp_get_unit_info(unit_id: int):
    data = []
    with db.create_sesion() as session:
        try:
            result = session.query( vw.VM_Unit_location_mapping_info).filter(vw.VM_Unit_location_mapping_info.unit_id == unit_id).all()
            print("Printing Result :", result[0])
        except StopIteration:
            raise ValueError ("Failed to fetch data. Procedure did not return any result")
        return result            

  
def stp_get_loc_unit_summary_for_user(user_id : int, location_id : int):
    sensor_details = []
    with db.create_sesion() as session:
        try:
            sensor_details = session.query(vw.VW_Metric_Summary_Latest_unit_Level).filter(vw.VW_Metric_Summary_Latest_unit_Level.user_id == user_id and location_id == location_id)
            return sensor_details
        except StopIteration:
            raise ValueError("Failed to Fetch data")
    return sensor_details

#
def stp_get_metric_data(id_user : int, id_metric :int, time_start : str, time_end :str):
    data = []
    with db.create_sesion() as session:
        query = f"call stp_get_metric_data({id_user}, {id_metric}, '{time_start}', '{time_end}')"
        data = session.execute(query).all()
    return data

#
def stp_get_all_unit_level_metrics(user_id : int , location_id : int , unit_id : int):
    data = []
    with db.create_sesion() as session:
        query = f"call stp_get_all_unit_level_metrics({user_id}, {location_id}, {unit_id})"
        data = session.execute(query).all()  
    return data

def warehouse_view_unit_summary(user_id : int , location_id : int):
    return stp_get_loc_unit_summary_for_user(user_id, location_id)

def metricview_metric_summary(user_id : int, metric_id : int, time_start , time_end):
    return stp_get_metric_data(user_id, metric_id, time_start, time_end)


def unit_view_unit_metrics(user_id : int, location_id : int , unit_id : int):
    response = []
    with db.create_sesion() as session:
        result = session.query(vw.VW_Metric_Summary_Latest_unit_Level).filter(
            vw.VW_Metric_Summary_Latest_unit_Level.user_id == user_id and
            vw.VW_Metric_Summary_Latest_unit_Level.location_id == location_id and
            vw.VW_Metric_Summary_Latest_unit_Level.unit_id == unit_id
            ).all()
        for i in result:
            response.append({
            "Metric Type": i.sensor_type,
            "Location Name": i.location_name,
            "Location Alias": i.location_alias,
            "Unit Name": i.unit_name,
            "Unit Alia": i.unit_alias,
            "Sensor Id": i.sensor_id,
            "Sensor Name": i.sensor_name,
            "Sensor Alias": i.sensor_alias,
            "Value": i.value,
            "Unit": i.unit,
            'Percentage' : 100, # need to calculate "Percentage",
            'State' : 'Normal',  #need to calcualte          
            "Value Duration Minutes": i.value_duration,
            'Threshold crosses' : 0, # need to calculate 'Threshold crosses'            
            })
    # data = stp_get_all_unit_level_metrics(user_id, location_id, unit_id)
    return response

def stp_get_locations_state_for_user(user_id : int):
    response = []
    with db.create_sesion() as session:
        try: 
            result = session.query(vw.VM_Unit_location_mapping_info).filter(vw.VM_Unit_location_mapping_info.user_id == user_id).all()
            for i in result:
                response.append({
                    'user_id': i.user_id,
                    'user_name': i.user_name,
                    'role': 'i.role', # need to create roles
                    'role_id': 'i.role_id', # need to create roles
                    'latitude': i.latitude,
                    'longitude': i.longitude,
                    'global_location_name': i.global_location_name,
                    'location_id': i.location_id,
                    'State' : 'Normal'
                })
        except StopIteration :
            raise ValueError("No Data Fetched")
        return response

def all_locations(user_id : int):
    try:
        loc_list = stp_get_locations_state_for_user(user_id)
    except StopIteration:
        raise ValueError("Failed to Fetch data. Procedure did not return any results.")

    return loc_list        

def data_for_metrics(user_id : int, location_id : int):
    data =  {
        'value' : 0,
        'state' : normality[0]
    }

    with db.create_sesion() as session:
        try:
            with db.create_sesion() as session:
                tmp = session.query(vw.VM_Unit_location_mapping_info).filter(vw.VM_Unit_location_mapping_info.user_id == user_id and vw.VM_Unit_location_mapping_info.location_id==location_id).all()
            # ending = ';' if location_id == 0 else f'and location_id = {location_id};'
            # tmp = session.execute(f'select * from vw_get_all_metrics_out_for_user where user_id = {user_id} {ending}').all()
            # if tmp is not None:
            #     if len(tmp) > 0:
            #         aggregated_counts = {
            #             nrm : sum([x.metrics_out_count for x in tmp if x.state == nrm])
            #             for nrm in normality
            #         }
        except StopIteration:
            raise ValueError("Failed to Fetch data.")
    print(f"fetching data fro {user_id} and {location_id}")
    # vw_get_all_metrics_out_for_user
    return tmp

def get_latest_metric_data(metric_id :int, user_id = int):
    response = []
    try:
        with db.create_sesion() as session:
            metric_info = session.query(vw.VW_Metric_Summary_Latest_unit_Level).filter(vw.VW_Metric_Summary_Latest_unit_Level.sensor_id == metric_id , vw.VW_Metric_Summary_Latest_unit_Level.user_id == user_id).all()
            for i in metric_info:
                response.append({
                    'metric_id': i.sensor_id,
                    'metric_name' : i.sensor_alias,
                    'location_id' : i.location_id,
                    'location_name' : i.location_name,
                    'unit_id' : i.unit_id,
                    'unit_name' : i.unit_name,
                    'is_warehouse_level_unit' : i.is_warehouse_level_unit,
                    'label' : i.sensor_name,
                    'measure_unit': i.unit,
                    'sensor_type': i.sensor_type})
    except StopIteration:
        raise ValueError("Failed to Fetch Data for metric")    
    return response

class User():
    def get_user_info_by_email(self, user_email):
        print(f"Gettin g user info for {user_email}")
        try:
            with db.create_sesion() as session:
                user_info = session.query(db.User_Master.user_name, db.User_Master.pw_hash, db.User_Master.user_id, db.User_Master.token ).filter(db.User_Master.user_name == user_email).one()
                print("User info found to be {user_info}")
            return {
                'email': user_info[0],
                'password' : user_info[1],
                'user_id' : user_info[2],
                'token' : user_info[3]
            }
        except exc.NoResultFound:
            user_info = None
            print(f"Exception caught as no info found for user")
    
    def get_user_id_with_email(self, user_email):
        try :
            with db.create_sesion() as session:
                user_id = session.query(db.User_Master.user_id).filter(db.User_Master.user_name == user_email).one()[0]
        except exc.NoResultFound:
            user_id = None
        print(user_id , " : Result")
        return user_id

    def add_new_user(self, user_info):
        try:
            with db.create_sesion() as session:
                user_obj = db.User_Master(
                    user_name = user_info['email'],
                    first_name = user_info['first_name'],
                    last_name = user_info['last_name'],
                    is_receive_alerts = True,
                    is_active = True,
                    pw_hash = user_info['password']
                )
                session.add(user_obj)
                session.commit()

        except Exception as e:
            print("Failed with exception :", e)


#  ----------------------------------------------------------------------------------------------------------




def get_user_list():
    response = []
    with db.create_sesion() as session:
        user_list = session.query(db.User_Master).options(orm.Load(db.User_Master)).all()
        for i in user_list:
            response.append({
                    "user_id" : i.user_id,
                "user_name" : i.user_name,
                "first_name" : i.first_name,
                "last_name" : i.last_name,
                "is_receive_alerts" : i.is_receive_alerts,
                "is_active" : i.is_active,
                "pw_hash" : i.pw_hash,
                "token" : i.token,
                "from_date" : i.from_date,
                "to_date" : i.to_date,
                "created_by" : i.created_by,
                "created_ts" : i.created_ts,
                "updated_by" : i.updated_by,
                "updated_ts" : i.updated_ts

                        })            




    return response

def run_test_query():
    with db.create_sesion() as session:
        location_list = session.query(db.User_Master).all()
    return [x.to_dict() for x in location_list]
    # return location_list