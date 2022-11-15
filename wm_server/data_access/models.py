from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Numeric, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

db_congig={
    "type" : "postgresql",
    "host":"134.209.158.162",
    "port":"5432",
    "user_name": "postgres",
    "password":"Genesis%40123",
    "db":"genesisdb2"
}

# db_congig = {
#     "type" : "mysql",
#     'host': '134.209.158.162',
#     "port":"3306",
#     #'MYSQL_HOST': 'localhost',
#     'user_name': 'GenesisServerApp',
#     'password': 'GenesisServerApp%40321',
#     'db': 'genesisdb',
# }

engine = create_engine(f"{db_congig['type']}://{db_congig['user_name']}:{db_congig['password']}@{db_congig['host']}:{db_congig['port']}/{db_congig['db']}")
print(engine)
Base = declarative_base()

def create_sesion():
    Session = sessionmaker()

    Session.configure(bind=engine)
    return Session()

# session = Session()



class Client_Master(Base):
    __tablename__ = 'client_master'
    client_id = Column(Integer, primary_key=True)
    client_name = Column(String)
    created_ts = Column(Integer)
    def to_dict(self):
        return {
            "client_id": self.client_id,
            "client_name": self.client_name,
            "created_ts": self.created_ts
        }

class User_Master(Base):
    __tablename__ = 'user_master'
    user_id = Column(Integer, primary_key = True, autoincrement = True)
    user_name = Column(String, nullable = False, unique = True)
    first_name = Column(String, nullable = False)
    last_name = Column(String, nullable = False)
    is_receive_alerts = Column(Boolean, nullable = False)
    is_active = Column(Boolean, nullable = False)
    pw_hash = Column(String)
    token = Column(String)
    from_date = Column(DateTime, nullable = False , default = datetime.utcnow)
    to_date = Column(DateTime)
    created_by = Column(String, nullable = False, default ='Admin')
    created_ts = Column(DateTime, nullable = False, default = datetime.utcnow)
    updated_by = Column(String)
    updated_ts = Column(DateTime)

    def to_dict(self):
        return {
                    "user_id" : self.user_id,
                "user_name" : self.user_name,
                "first_name" : self.first_name,
                "last_name" : self.last_name,
                "is_receive_alerts" : self.is_receive_alerts,
                "is_active" : self.is_active,
                "pw_hash" : self.pw_hash,
                "token" : self.token,
                "from_date" : self.from_date,
                "to_date" : self.to_date,
                "created_by" : self.created_by,
                "created_ts" : self.created_ts,
                "updated_by" : self.updated_by,
                "updated_ts" : self.updated_ts

                        }

class User_location_unit_map(Base):
    __tablename__ = 'user_location_unit_map'    
    user_location_unit_id = Column(Integer, primary_key=True, nullable = True, autoincrement= True)
    user_id = Column(Integer, ForeignKey('user_master.user_id'))
    location_id = Column(Integer, ForeignKey('location_master.location_id'))
    unit_id = Column(Integer, ForeignKey('location_master.location_id'))
    is_active = Column(Boolean, nullable = False, default= True)
    from_date = Column(DateTime, nullable = False , default = datetime.utcnow)
    to_date = Column(DateTime)
    created_by = Column(String, nullable = False, default ='Admin')
    created_ts = Column(DateTime, nullable = False, default = datetime.utcnow)
    updated_by = Column(String)
    updated_ts = Column(DateTime)

    def to_dict(self):
        return {
            "user_location_unit_id": self.user_location_unit_id,
            "user_id": self.user_id,
            "location_id" : self.location_id,
            "unit_id" : self.unit_id,
            "is_active" : self.is_active,
            "from_date" : self.from_date,
            "to_date" : self.to_date,
            "created_by" : self.created_by,
            "created_ts" : self.created_ts,
            "updated_by" : self.updated_by,
            "updated_ts" : self.updated_ts

        }

class Unit_Master(Base):
        __tablename__ = 'unit_master'
        unit_id = Column( Integer, primary_key = True, autoincrement = True)
        global_unit_name = Column(String, nullable = False, unique= True)
        unit_alias = Column(String, unique = True)
        is_active = Column(Boolean)
        is_warehouse_level_unit = Column(Boolean, default= False)
        from_date = Column(DateTime, nullable = False , default = datetime.utcnow)
        to_date = Column(DateTime)
        created_by = Column(String, nullable = False, default ='Admin')
        created_ts = Column(DateTime, nullable = False, default = datetime.utcnow)
        updated_by = Column(String)
        updated_ts = Column(DateTime)
        def to_dict(self):
            return {
                "unit_id": self.unit_id,
                "global_unit_name": self.global_unit_name,
                "unit_alias" : self.unit_alias,
                "is_warehouse_level_unit" : self.is_warehouse_level_unit,
                "is_active" : self.is_active,
                "from_date" : self.from_date,
                "to_date" : self.to_date,
                "created_by" : self.created_by,
                "created_ts" : self.created_ts,
                "updated_by" : self.updated_by,
                "updated_ts" : self.updated_ts

            }

class Location_Master(Base):
    __tablename__ = 'location_master'
    location_id = Column(Integer, primary_key = True, nullable = False, autoincrement = True, unique = True)
    global_location_name = Column(String, nullable = False, unique = True)
    location_alias = Column(String, unique = True)
    latitude = Column(Numeric , nullable = False)
    longitude = Column(Numeric, nullable = False)
    is_active = Column(Boolean, nullable = False, default= True)
    from_date = Column(DateTime, nullable = False , default = datetime.utcnow)
    to_date = Column(DateTime)
    created_by = Column(String, nullable = False, default ='Admin')
    created_ts = Column(DateTime, nullable = False, default = datetime.utcnow)
    updated_by = Column(String)
    updated_ts = Column(DateTime)
    is_disabled = Column(Boolean, default = datetime.utcnow)
    def to_dict(self):
        return{
                "location_id" : self.location_id,
    "global_location_name" : self.global_location_name,
    "location_alias" : self.location_alias,
    "latitude" : self.latitude,
    "longitude" : self.longitude,
    "is_active" : self.is_active,
    "from_date" : self.from_date,
    "to_date" : self.to_date,
    "created_by" : self.created_by,
    "created_ts" : self.created_ts,
    "updated_by" : self.updated_by,
    "updated_ts": self.updated_ts,
    "is_disabled" : self.is_disabled
        }

