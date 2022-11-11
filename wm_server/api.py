from datetime import datetime
from lib2to3.pgen2 import token
from time import sleep
from urllib import response
from fastapi.security import OAuth2PasswordRequestForm
from fastapi import FastAPI, status, HTTPException, Depends, WebSocket , Query
from uuid import uuid4
import json

from fastapi.testclient import TestClient

from .shared.schemas import UserAuth, UserOut, TokenSchema, SystemUser, Report_Interval
from .data_access import db_queries as db
from .shared.utils import get_hashed_password, verify_password, create_access_token, create_refresh_token
from .shared.deps import get_current_user , get_user_info_from_token

from .business_logic import user_resources

app = FastAPI()

async def websocket_connect(websocket, msg):
    await websocket.accept()
    await websocket.send_json(msg)
    await websocket.close()


@app.get('/locations', summary = 'Get details of currently logged in user')
async def user_location(user: SystemUser = Depends(get_current_user)):
    return user_resources.UserLocation().get( user.user_id )

@app.websocket('/metrics/warehouse/{warehouse_id}')
async def websocket_endpoint( warehouse_id : int ,websocket: WebSocket , token: str = Query(...) ):
    user_info : SystemUser =  await get_user_info_from_token(token=token)
    await websocket_connect(websocket, user_resources.Metrics_Warehouse().get(warehouse_id, user_info.user_id) )


@app.get('/locations/{location_id}/summary', summary = 'Get details of current user location')
async def less_qty(location_id : int, user : SystemUser = Depends(get_current_user)):
    return user_resources.Location_Summary().get(location_id, user.user_id)


@app.websocket('/metrics/warehouse/{warehouse_id}/unit/{unit_id}')
async def websocket_endpoint( warehouse_id : int, unit_id : int ,websocket: WebSocket , token: str = Query(...) ):
    user_info : SystemUser =  await get_user_info_from_token(token=token)
    await websocket_connect(websocket, user_resources.Metrics_Unit().get(warehouse_id, unit_id,user_info.user_id) )



@app.post('/api/history/{metric_id}', summary = 'Get details of metric')
async def metric_chart_history(metric_id : int, parameter : Report_Interval,  user : SystemUser = Depends(get_current_user)):
    return user_resources.MetricsChartHistory().get(metric_id, user.user_id, parameter)

@app.post('/login',summary = 'Create access and refresh tokens for user', response_model = TokenSchema)
async def login(from_data : OAuth2PasswordRequestForm = Depends()):
    try:
        user = db.User().get_user_info_by_email(from_data.username)
    except TypeError:
        user = None
    if user is None:
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = "Incorrect email or user_name"
        )
    hashed_password = user['password']
    if not verify_password(from_data.password, hashed_password):
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = "Incorrect Email or password"
        )
    access_token = create_access_token(user['email'])
    db.update_token_for_user(user['user_id'], access_token)
    return {
        "access_token": access_token,
        "refresh_token": create_refresh_token(user['email']),
    }

@app.post('/signup', summary = "Create new user")
async def create_user(data: UserAuth):
    try:
        user = db.User().get_user_info_by_email(data.email)
        print(f"user info found are {user}")
    except TypeError:
        user = None
        print(f"user info found are {user}")
    if user is not None:
        print(f"User Email id : {data.email} is already being used")
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = "Email ID is already been used"
        )
    user = {
        'email' : data.email,
        'password' : get_hashed_password(data.password),
        'id' : str(uuid4()),
        'first_name': data.first_name,
        'last_name' : data.last_name

    }
    print('User details:',user)
    db.User().add_new_user(user)
    return user

# -----------------------------------------------------------------------------


@app.get('/users')
async def get_all_user_name():
    return user_resources.User().user_name_list()

@app.get('/all_unit')
async def get_all_unit():
    response =[]
    with db.db.create_sesion() as session:
        unit_list = session.query(db.db.Unit_Master).options(db.orm.Load(db.db.Unit_Master)).all()
        for i in unit_list:
            response.append({
                "unit_id": i.unit_id,
                "global_unit_name": i.global_unit_name,
                "unit_alias" : i.unit_alias,
                "is_warehouse_level_unit" : i.is_warehouse_level_unit,
                "is_active" : i.is_active,
                "from_date" : i.from_date,
                "to_date" : i.to_date,
                "created_by" : i.created_by,
                "created_ts" : i.created_ts,
                "updated_by" : i.updated_by,
                "updated_ts" : i.updated_ts

            })
        return response

@app.get('/all_user_location_maping')
async def get_all_user_location_mapping():
    with db.db.create_sesion() as session:
        user_location_mapping_info = session.query(db.db.User_location_unit_map).options(db.orm.Load(db.db.User_location_unit_map)).all()
        return user_location_mapping_info


# ---------------------------------------------------------------------------------------
@app.get('/check_websocket_wareshouse')
async def check_websocket_wareshouse(user : SystemUser = Depends(get_current_user)):
    client = TestClient(app)
    warehouse_id = 1
    with client.websocket_connect(f'/metrics/warehouse/{warehouse_id}?token={user.token}') as websocket:
        data = json.loads(websocket.receive_json())
        print(data)
        # print('data recived from websocket:', data)
    return data

@app.get('/check_websocket')
async def check_websocket(user : SystemUser = Depends(get_current_user)):
    client = TestClient(app)
    warehouse_id = 1
    unit_id = 1
    with client.websocket_connect(f'/metrics/warehouse/{warehouse_id}/unit/{unit_id}?token={user.token}') as websocket:
        data = json.loads(websocket.receive_json())
        print(data)
        # print('data recived from websocket:', data)
    return data
       

