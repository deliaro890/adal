from jwt import encode , decode , exceptions
from datetime import datetime ,timedelta , timezone
import os 
from dotenv import load_dotenv

from fastapi.responses import JSONResponse


#load_dotenv(os.path.join(project_folder, '.env'))
load_dotenv("variables.env")

def expire_date (minutes : int):
    date = datetime.now(tz=timezone.utc)
    new_date = date + timedelta(minutes=minutes)
    return new_date


def write_token (data : dict):
    token = encode (payload = {**data, "exp" : expire_date (20) } , key = os.getenv("SECRET") , algorithm = "HS256")
    return token



def validate_token(token , output = False ):
    try :
        
        print ("decodificando")
        if output :
            print (decode (token, key = os.getenv("SECRET") ,algorithms = ["HS256"] ))
            print ("decodificado")
            return True
        print (decode (token, key = os.getenv("SECRET") ,algorithms = ["HS256"] ))
        print ("decodificado")

    except exceptions.DecodeError:
        return JSONResponse (content = {"mesage": "Invalid Token"} , status_code = 401 )
    except exceptions.ExpiredSignatureError:
        return JSONResponse (content = {"mesage": "Token Expired"} , status_code = 401 )
    