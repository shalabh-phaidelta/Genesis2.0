from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field

class TokenSchema(BaseModel):
    access_token: str
    refresh_token: str
    
    
class TokenPayload(BaseModel):
    sub: str = None
    exp: int = None


class UserAuth(BaseModel):
    email: str = Field(..., description="user email")
    password: str = Field(..., min_length=5, max_length=24, description="user password")
    first_name: str = Field(..., description="first_name")
    last_name: str = Field(..., description="first_name")


class UserOut(BaseModel):
    id: UUID
    email: str
    user_id : int


class SystemUser(UserOut):
    password: str
    token : str

class Report_Interval(BaseModel):
    start_time : str
    end_time : str
    interval : str

