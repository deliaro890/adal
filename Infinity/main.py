from fastapi import FastAPI , Request
from dotenv import load_dotenv
import os 
from routes.auth import auth_routes
from routes.users_github import users_github
from functions_jwt import validate_token
from middlewares.ratelimit import  RateLimitingMiddleware

from typing import Annotated
from fastapi import Depends, Header, HTTPException

from fastapi.middleware.cors import CORSMiddleware

load_dotenv()
app = FastAPI()

origins = [
    "http://172.20.10.2:8080",  # La dirección de tu frontend Vuestic
     "http://localhost:8080"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(auth_routes , prefix = "/api")

@app.get("/")
def read_root():
    return {"message": "Hello World!"}


@app.get("/items/")
async def read_items(request : Request):
    token = request.headers["Authorization"].split(" ")[1]
    print (token)
    if validate_token(token, True) == True:
        print("token valido")
        pass
    else:
       return validate_token(token, True)        
    return [{"item": "Foo"}, {"item": "Bar"}]

app.include_router(users_github , prefix = "/api" )

app.add_middleware(RateLimitingMiddleware)




