from typing import Union, Any
from datetime import datetime
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, HTTPBearer, HTTPAuthorizationCredentials
from uuid import uuid4
from jose import jwt
from pydantic import ValidationError

from .utils import (ALGORITHM, JWT_SECRET_KEY)


from .schemas import TokenPayload, SystemUser
from . . data_access import db_queries as db
# from .. import data_access as db
# from replit import db

# reuseable_oauth = OAuth2PasswordBearer( tokenUrl= "/login", scheme_name="JWT")

async def get_user_info_from_token(token):
    try :
        print("token :- ", token)
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        token_data = TokenPayload(**payload)
        if datetime.fromtimestamp(token_data.exp) < datetime.now():
            raise HTTPException(
                status_code= status.HTTP_401_UNAUTHORIZED,
                detail= "Token expired",
                headers={"WWW-Authenticate": "Bearer"}
            )        

    except (jwt.JWTError, ValidationError):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="could not validate credentials",
            headers= {"WWW-Authenticate": "Bearer"}
        )


    user : Union[dict[str, Any], None] = db.User().get_user_info_by_email(token_data.sub)

    user['id']=str(uuid4())

    if user is None:
        raise HTTPException(
            status_code= status.HTTP_404_NOT_FOUND,
            detail="Could not find user",
        )

    return SystemUser(**user)        


# async def get_current_user(token : str = Depends(reuseable_oauth)):
#     try:
#         print("token : ", token)
#         payload = jwt.decode(
#             token , JWT_SECRET_KEY, algorithms=[ALGORITHM]
#         )
#         token_data = TokenPayload(**payload)


#         if datetime.fromtimestamp(token_data.exp) < datetime.now():
#             raise HTTPException(
#                 status_code= status.HTTP_401_UNAUTHORIZED,
#                 detail= "Token expired",
#                 headers={"WWW-Authenticate": "Bearer"}
#             )
#     except (jwt.JWTError, ValidationError):
#         print("Error is here in token ")
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="could not validate credentials",
#             headers= {"WWW-Authenticate": "Bearer"}
#         )


#     user : Union[dict[str, Any], None] = db.User().get_user_info_by_email(token_data.sub)
#     print('User info is extracted',user)

#     user['id']=str(uuid4())

#     if user is None:
#         print("Error is here in user_authentication")
#         raise HTTPException(
#             status_code= status.HTTP_404_NOT_FOUND,
#             detail="Could not find user",
#         )
#     print("Returning user info")
#     print(SystemUser(**user))
#     return SystemUser(**user)        


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(JWTBearer, self).__call__(request)
        if credentials:
            if not credentials.scheme == "Bearer":
                raise HTTPException(status_code=403, detail="Invalid authentication scheme.")
            return credentials.credentials
        else:
            raise HTTPException(status_code=403, detail="Invalid authorization code.")


async def get_current_user(token : str = Depends(JWTBearer())):
    try:
        payload = jwt.decode(
            token , JWT_SECRET_KEY, algorithms=[ALGORITHM]
        )
        token_data = TokenPayload(**payload)


        if datetime.fromtimestamp(token_data.exp) < datetime.now():
            raise HTTPException(
                status_code= status.HTTP_401_UNAUTHORIZED,
                detail= "Token expired",
                headers={"WWW-Authenticate": "Bearer"}
            )
    except (jwt.JWTError, ValidationError):
        print("Error is here in token ")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="could not validate credentials",
            headers= {"WWW-Authenticate": "Bearer"}
        )


    user : Union[dict[str, Any], None] = db.User().get_user_info_by_email(token_data.sub)
    print('User info is extracted',user)

    user['id']=str(uuid4())

    if user is None:
        print("Error is here in user_authentication")
        raise HTTPException(
            status_code= status.HTTP_404_NOT_FOUND,
            detail="Could not find user",
        )
    print("Returning user info")
    print(SystemUser(**user))
    return SystemUser(**user)        



