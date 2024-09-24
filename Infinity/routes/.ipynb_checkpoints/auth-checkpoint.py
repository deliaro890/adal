from fastapi import APIRouter , Header
from pydantic import BaseModel , EmailStr
from functions_jwt import write_token , validate_token
from fastapi.responses import JSONResponse

auth_routes = APIRouter()

class User(BaseModel):
    username : str 
    email : EmailStr



@auth_routes.post ("/login")
def log_in(user: User):
    print (user.dict())
    if user.username == "Adal" :
        return write_token (user.dict())
    else :
        return JSONResponse (content = {"message: ": "user not found"} , status_code = 404)
        

@auth_routes.post("/verify/token")
def verify_token(Authorization : str = Header(None) ):
    token = Authorization.split(" ")[1]
    
    
    return validate_token(token,True)