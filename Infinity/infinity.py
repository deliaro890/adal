# %%
#ID_SHEET='1jOYfMBs1hXSHrcPTGCL6D-nCgRRbI7beDLV4VbfV_pU'
#APIKEY='AIzaSyCjU9evRYuLBYWDPAZeMmrERb7pwm6rWqs'

# %%
import gspread
import pandas  as pd
from os import environ
from google.cloud import bigquery
#from random import random #Debug
import  traceback
from datetime import datetime ,timedelta 
from time import sleep
import numpy
from fastapi import FastAPI ,Request , Header
from pydantic import BaseModel
from typing import Optional
from functions import *
import json
from fastapi.responses import JSONResponse
from middlewares.ratelimit import  RateLimitingMiddleware
from functions_jwt import validate_token
import os

from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://172.20.10.2:8080",  # La dirección de tu frontend Vuestic
    "http://172.20.10.2,
    "http://localhost:8080",
    "http://localhost",
    "http://80.233.48.17:8080", # ip publica de mi mac
    "http://80.233.48.17",
    "http://172.20.10.2", # ip de mi mac de mi red local
    "http://172.20.10.15"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Cargar el archivo .env
load_dotenv()

#environ["GOOGLE_APPLICATION_CREDENTIALS"]="../keys_g/nova-cel-bot.json"
credentials_path_1 = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_1")
client = bigquery.Client.from_service_account_json(credentials_path_1)
#client = bigquery.Client()
#sa= gspread.service_account(filename='../keys_g/sheetaccount.json')
#sh =sa.open("Registro Nova cel (Responses)")

# %%
#client.close()

# Registra la función no la modifica

class Usuario(BaseModel):
    """Clase Creada de registros para validación de parametros de entrada"""
    
    email : str #correo
    name : str# Nombre
    last_name : str  #apellido
    age : int #Edad
    country_lada : str#lada del país 
    phone : str# telefono
    gender : str #genero
    url_avatar : str # URL del avatar
    password : str

class Correo(Request):
    """Clase Creada de registros para validación de parametros de entrada"""
    email : str #correo

class CorreoCode(Request):
    """Clase Creada de registros para validación de parametros de entrada"""
    email : str #correo
    code : int 

class Login(BaseModel):
    """Clase Creada de registros para validación de parametros de entrada"""
    email : str #correo
    password : str

class Ident(BaseModel,Request):
    """Clase para las funciones que solo requiren id """
    id : int

def valida (request : Request):
    token = request.headers["Authorization"].split(" ")[1]
    print (token)
    if validate_token(token, True) == True:
        print("token valido")
        pass
    else:
       return validate_token(token, True)

@app.get("/")
def index():
    """Ruta principal"""
    print("Servidor OK")
    return {"mensaje": "Servidor OK ve a http://127.0.0.1:8000/docs"}




@app.post("/crea_nuevo_usuario")
def create_new_user ( datos : Usuario):
    """Ejemplo: {
      "email": "algo@dominio.com",
      "name": "Fulanito",
      "last_name": "Perez",
      "age": 33,
      "country_lada": "+52",
      "phone": "5571784852",
      "gender": "H",
      "url_avatar": "http://www.avatars/avatar.png",
      "password": "Contraseña3*" 
    }
    El date_time_created = fecha tiempo actual America/Mexico_City , el id y el paid_positions = 0 se ponen automaticamente
    """
    
    diccionario = ({
        "email" : datos.email,
        "name" : datos.name,
        "last_name" : datos.last_name,
        "age" : datos.age ,
        "country_lada" : datos.country_lada,
        "phone" : datos.phone,
        "gender" : datos.gender,
        "url_avatar" : datos.url_avatar,
        "password" : datos.password    
    })


    return create_user(diccionario,client) 

@app.post("/return_user")
async def return__user(datos : Correo):
    """Retorna todos los datos de un user, dado su email"""
    datos_ = await datos.json()
    return return_user(datos['email'], client)


@app.put("/actualiza_usuario")
def update__user ( datos : Usuario):
    """Actualiza un usuario dado un correo, todos los campos son requeridos 
      Ejemplo: { \
      "email": "algo@dominio.com", \
      "name": "Fulanito", \
      "last_name": "Perez", \
      "age": 33, \
      "country_lada": "+52", \
      "phone": "5571784852", \
      "gender": "H", \
      "url_avatar": "http://www.avatars/avatar.png", \
      "password": "Contraseña3*"  \
    } \
    El date_time_created , el id y el paid_positions no se actualizan
    """
    
    diccionario = ({
        "email" : datos.email,
        "name" : datos.name,
        "last_name" : datos.last_name,
        "age" : datos.age ,
        "country_lada" : datos.country_lada,
        "phone" : datos.phone,
        "gender" : datos.gender,
        "url_avatar" : datos.url_avatar,
        "password" : datos.password    
    })


    return update_user(diccionario,client)


@app.patch("/agregar_pago")
async def add_pays(datos : Correo):
    """ se require el correo ejemplo  {"email": "fulanito@dominio.com"} , es para agregar pagos """
    if valida(datos) != None :
        return  valida (datos)        
    datos = await datos.json()
    
    return add_pay_position(datos['email'], client)

@app.post("/login_user")
async def login(datos : Login) :
    """ Regresa todos los datos del usuario siempre que exista su correo y contraseña en la base de datos"""
    datos = datos.dict()
   
    return login_user(datos['email'],datos['password'], client)

@app.post("/manda_codigo")
async def send_email_code(datos : Correo):
    """ se require el correo ejemplo  {"email": "fulanito@dominio.com"}"""
    if valida(datos) != None :
        return  valida (datos)        
    datos = await datos.json()

    return  send_code (datos["email"], client)

@app.get("/verificar_codigo")
async def verifica (correocode : CorreoCode) :
    """ se requiere mandar un correo valido y el útimo código que le fue enviado"""
    if valida(correocode) != None :
        return  valida (correocode)
    datos = await correocode.json() #El body 
    print (datos["email"] , datos["code"]) 
    return verify_code(datos["email"], datos["code"], client)

@app.post("/agrega_transaccion")
async def new_transact(datos : Ident):
    """Una vez pagada su posición se le asigna un registro y se modifican aquiellos que estén vinvulados, 2 registros arriba en la estructura , solo requiere el id del usuario a entrar """
    #token = datos.headers["Authorization"].split(" ")[1]
    #print (token)
    if valida(datos) != None :
        return  valida (datos)
        
    datos = await datos.json()
    print( datos['id'] )
    return insert_new_transact( datos['id'] , client )

@app.get("/user_info")
async def user_info(datos : Ident ):
    """ Regresa todos los datos de la estructura / transacciones del usuario dado su id """
    if valida(datos) != None :
        return  valida (datos)

    #print(request.receive())
    #return info_user_by_id ( id.id ,client )
    #print ( request.url )
    #print (request.base_url)
    #print (request.query_params)
    #print (request.path_params)
    #print (request.cookies)
    #print (request.client)
    # https://fastapi.tiangolo.com/reference/request/?h=request+class#fastapi.Request.json
    #id = await request.json() #El body 
    
    datos = await datos.json()
    return info_user_by_id ( datos["id"] ,client )

app.add_middleware(RateLimitingMiddleware)


#def login(datos : Login):

#    diccionario = ({
#        "email" : datos.email,
#        "password" : datos.password    
#    })

#    return login_user(diccionario['email'], diccionario['password'], client)



   
# %%
#insert_new_transact(n)



# %%
#print('Bienvenido')
#while True:
#    option=input('seleccione una opcion \n 1 : Actualizar Base de Usuarios \n 2 : Crear Estructura de usuarios \
#    \n 3 : Asociar usuarios \n 4 : Break Away de Usuario \n ' )
#    
#    if int(option) == 1:
#        update_users() 
#    elif int(option) == 2:
#        insert_new_transact()
#    elif int(option) == 3:
#        update_transact()
#    elif int(option) == 4:
#        break_away()
    

# %%
#break_away()



# %%
#pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# %%
#pip install google_spreadsheet


# %%
#pip install google-auth-oauthlib


# %%
#pip install gspread


# %%
#pip install google-cloud-bigquery

# %%
#configurar con sudo jupyter serverextension enable --py jupyterlab --sys-prefix

# %%
# Activar en sercidor con 
#jupyter lab --ip 0.0.0.0 --port 8888 --no-browser

# %%
#pip install db_dtypes

# %%
#pip install ipynb-py-convert

# %%
#pip install fastapi

# %%
#pip install "uvicorn[standard]"

# %%
#pip install jupyterlab-gitlab

# %%
#pip install jupyterlab-git

# %%
#pip install ipynb-py-convert

# %%
