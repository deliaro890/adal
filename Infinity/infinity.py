import pandas  as pd
from os import environ
from google.cloud import bigquery
#from random import random #Debug
import  traceback
from datetime import datetime ,timedelta 
from time import sleep
import numpy
from fastapi import FastAPI ,Request , Header
from typing import Optional
from functions import *
from models import Usuario, Correo, CorreoCode, Login, Ident, Ident2, Usuario2, Login_Google
import json
from fastapi.responses import JSONResponse
from middlewares.ratelimit import  RateLimitingMiddleware
from functions_jwt import validate_token
import os
from starlette.concurrency import run_in_threadpool

from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://172.20.10.2:8080",  # La dirección de tu frontend Vuestic
    "http://172.20.10.2",
    "http://localhost:8080",
    "http://127.0.0.1:8080", # no lo toma igual que el localhost!
    "http://localhost",
    "http://80.233.48.17:8080", # ip publica de mi mac
    "http://80.233.48.17",
    "http://172.20.10.2", # ip de mi mac de mi red local
    "http://172.20.10.15",
    "http://172.17.0.1",
    "http://vuestic-app:8080",
    "http://vuestic-app",
    "http://vuestic-app_1:8080",
    "http://vuestic-app_1",
    "http://187.191.36.188:8080",
    "*"

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


def valida (request : Request):
    try:
        token = request.headers["Authorization"].split(" ")[1]
    except:
        return JSONResponse(content={"message": "la peticion no tiene token"},status_code=400)
    print (token)
    if validate_token(token, True) == True:
        print("token valido")
        pass
    else:
       return validate_token(token,request['email'], True)

@app.get("/")
def index():
    """Ruta principal"""
    print("Servidor OK")
    return {"mensaje": "Servidor OK ve a http://127.0.0.1:8000/docs o al host:8000/docs"}

@app.post("/valida_token")
def validar_token(datos : Request) :

    return valida(datos)



@app.post("/crea_nuevo_usuario")
def create_new_user ( datos : Usuario):
    """crea un nuevo usuario.

    No es una función asincrona porque cada usuario debe de poseer un único id

        INPUT: 
        {
      "email": "string",
      "name": "string",
      "last_name": "string",
      "age": int,
      "country_lada": "string" length max 4,
      "phone": "string",
      "gender": "string H/M",
      "url_avatar": "string",
      "password": "string"
        }

    Ejemplo:{
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
    
    OUTPUT:{
  "message": "New User Created with id : <int>"
    } status code :200

    OUTPUT2: {
  "message":"correo invalido"
    } status code:400

    OUTPUT3: {
  "message": "correo <string> ya registrado"} status code:409

     OUTPUT4: {
  "message": "Something wrong ",
  "log ": "string"} status code: 400 ó 500
    """

    diccionario = datos.dict()

    return create_user(diccionario,client) 

@app.post("/return_user")
async def return__user(datos : Correo):
    """Retorna todos los datos de un user, dado su email"""
    datos_ = await datos.json()
    return return_user(datos['email'], client)


@app.put("/actualiza_usuario")
async def update__user ( datos : Usuario2):
    """Actualiza un usuario dado un correo, todos los campos son requeridos

        INPUT: {
        "email": "string",
        "name": "string",
        "last_name": "string",
        "age": int,
        "country_lada": "string" length max 4,
        "phone": "string",
        "gender": "string H/M",
        "url_avatar": "string",
        "password": "string"
          }

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
    Header "Authorization": "string" #JWT token, el token te lo da el login
    El date_time_created , el id y el paid_positions no se actualizan

    OUTPUT: {"message": "usuario actualizado", \
    "log": "DmlStats(inserted_row_count=0, deleted_row_count=0, updated_row_count=2)"} status code : 200 \
    
    OUTPUT2:{"message": "la peticion no tiene token"}, status code: 400 \
    
    OUTPUT3:{"mesage": "Invalid Token"} , status_code : 401 \
    
    OUTPUT4:{"mesage": "Token Expired"} , status_code = 401 \
    
    OUTPUT5:{"message": "correo <string> no existe"} status_code = 409\
    
    OUTPUT6:{"message": "correo invalido"} , status_code = 400 \
    
    OUTPUT7:{"message" : "no se pudo actualizar" }, status_code = 500  \
    
    OUTPUT8:{"message" : "algo salio mal ", "log ": <string>}, status_code = 500 \


    """
    
    print(datos)
    if  valida(datos) != None :
        return  valida(datos)
    datos = await datos.json() 
    diccionario =datos
    print(diccionario)
    return  update_user(diccionario,client)


@app.patch("/agregar_pago")
async def add_pays(datos : Correo):
    """ Agrega pago en la base de datos.

          INPUT:{"email": "string"}
          Header "Authorization": "string" #JWT token, el token se obtiene en el login

    OUTPUT:{
    "message": "pago de posición agregado a cliente",
    "log": "string""
    } status code = 200

    OUTPUT2:{"message": "la peticion no tiene token"}, status code: 400 
    
    OUTPUT3:{"mesage": "Invalid Token"} , status_code : 401 

    OUTPUT4:{"mesage": "Token Expired"} , status_code = 401 

    OUTPUT5:{"message": "correo <string> no encontrado"} status_code = 404

    OUTPUT6:{"message": "correo invalido"} , status_code = 400 

    OUTPUT7:{"message" : "Something wrong", "log ": <string>}, status_code = 500 
        """
    if valida(datos) != None :
        return  valida (datos)        
    datos = await datos.json()
    
    return add_pay_position(datos['email'], client)

@app.post("/login_user")
async def login(datos : Login) :
    """ Regresa todos los datos del usuario siempre que exista su correo y contraseña en la base de datos

    Input: {
    "email": "string",
    "password": "string"
      }
    
  Output: {
  "date_time_created": {
    "0": "YYYY-MM-DDTHH:MM:SS.ssssss"
  },
  "email": {
    "0": "string"
  },
  "name": {
    "0": "string"
  },
  "last_name": {
    "0": "string"
  },
  "age": {
    "0": int
  },
  "country_lada": {
    "0": "string"
  },
  "phone": {
    "0": "string"
  },
  "gender": {
    "0": "string H/M"
  },
  "url_avatar": {
    "0": "url string"
  },
  "id": {
    "0": int
  },
  "password": {
    "0": "string"
  },
  "paid_positions": {
    "0": int 
  },
  "email_verified": {
    "0": null  #Not in use
  },
  "email_code": {
    "0": null #Not in use
  },
  "Authorization": "string" #JWT token
}  
Status code 200

Output 2 : {
  "message": "correo <string> no encontrado"
}
Status code 404

output 3 : {
  "message": "contraseña invalida"
}
Status code 401

    """
    datos = datos.dict()
    
    return login_user(datos['email'],datos['password'], client)

@app.post("/login_user_google")
async def login_google(datos : Login_Google) :
    """ Regresa todos los datos del usuario y el JWT siempre que exista su correo en la base de datos
    si no existe el usuairo entonces solo regresa wl JWT y el front lo debe de registrar usando el JWT obtenido usando /crea_nuevo_usuario

    token_google : es el proporcionado por google    
    client_id : es el proporcionado por google
    

    Input: {
    "token_google": "string",
    "client_id": "string"
      }
    
  Output: {
  "date_time_created": {
    "0": "YYYY-MM-DDTHH:MM:SS.ssssss"
  },
  "email": {
    "0": "string"
  },
  "name": {
    "0": "string"
  },
  "last_name": {
    "0": "string"
  },
  "age": {
    "0": int
  },
  "country_lada": {
    "0": "string"
  },
  "phone": {
    "0": "string"
  },
  "gender": {
    "0": "string H/M"
  },
  "url_avatar": {
    "0": "url string"
  },
  "id": {
    "0": int
  },
  "password": {
    "0": "string"
  },
  "paid_positions": {
    "0": int 
  },
  "email_verified": {
    "0": null  #Not in use
  },
  "email_code": {
    "0": null #Not in use
  },
  "Authorization": "string" #JWT token
}  
Status code 200

OUTPUT 2 : {"message": "verifica tu email"} , status_code = 401 

OUTPUT 3 : {"message": "valor invalido {}".format(str(e)) } , status_code = 400 )
sucede cuando el token ha expirado u otro valor es invalido

OUTPUT 4 : {"message": "algo salió mal en el token de google", "log": "string"} , status_code = 500 

OUTPUT 5: {"message": "correo invalido"} , status_code = 400 , cuando la estructura del correo es invalida

OUTPUT 6: {"Authorization":"string" #JWT token, "message": "correo string no encontrado}, status_code = 404 , el front debe de invocar a /crea_nuevo_usuario

OUTPUT 7: {"message" : "algo salio mal " , "log" : string } , status_code = 500


    """
    datos = datos.dict()
    
    return login_user_google(datos['token_google'],datos['client_id'], client)


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

@app.post("/agrega_estructura")
def new_block_structure(datos : Ident2):
    """Una vez pagada su posición se le asigna un registro (función insert_new_structure) 
    y se modifican aquellos que estén vinvulados 2 registros arriba en la estructura (función update_structure),
    solo requiere el id del usuario a entrar. 
    Es una funcion sincrona porque se agregan a la estructura global de uno por uno.
    No requiere token porque es lo que se debe de hacer automaticamente al pagar.

    INPUT: {"id": int}

    OUTPUT: {"message": "ok Done" }, status_code: 200 # Esto pasa con los primeras 3 posiciones 

    OUTPUT2:{"message": "Usuario asociado correctamente"} status code : 200

    OUTPUT3: {"message": "hay que pagar la posición primero"} status code : 402

    OUTPUT : {"message" : "Error , El usuario con el ID: int no se encuentra registrado en el registro de usuarios" }, status_code = 404

    OUTPUT4: { "message" : "Error El usuario con el ID: int no se encuentra registrado en el registro de estructura favor de agregarlo" }, status code: 404

    OUTPUT5: {"message" : "El ID ingresado no tiene estructura favor de crearla"}, status_code : 409 

    OUTPUT6: {"message" :"El registro no se pudo crear"}, status_code : 500

    OUTPUT6: { "message": 'Usuario no encontrado en base de estructuras, favor de agregarlo'}, status_code:500

    OUTPUT7:{"message" : "El usuario int ya tiene asociados a los usuarios int y int , termine su ciclo para iniciar uno nuevo"}, status_code : 500

    OUTPUT8: {"message": "parece que hay más de un registro con el mismo id en la misma posicion , revise la base de datos!!!"} , status_code : 500

    OUTPUT9: {"message": 'El usuario no se pudo asociar ' }, status_code :500  
       
    """
    #token = datos.headers["Authorization"].split(" ")[1]
    #print (token)
    #if valida(datos) != None :
    #    return  valida (datos)   
    datos = datos.dict()
   #print( datos['id'] )
    return insert_new_structure( datos['id'] , client )

@app.get("/user_info_structure")
async def user_info(datos : Ident ):
    """ Regresa todos los datos de la estructura del usuario dado su id para mayor referencia consute el grafico  https://lookerstudio.google.com/reporting/1b848ddf-958a-4fcb-a0e3-fe37c634a81e
        INPUT: {"id": int }
         "Authorization": "string" #JWT

      OUTPUT:{
    "ID": dict[int],
    "position": dict[int],
    "name": dict[string],
    "email": dict[string],
    "B": dict[int],
    "C": dict[int],
    "D": dict[int],
    "E": dict[int],
    "F": dict[int],
    "G": dict[int],
    "uplineID": dict[int],
    "avatar_A": dict[string],
    "nombre_B": dict[string],
    "avatar_B": dict[string],
    "nombre_C": dict[string],
    "avatar_C": dict[string],
    "nombre_D": dict[string],
    "avatar_D": dict[string],
    "nombre_E":dict[string],
    "avatar_E": dict[string],
    "nombre_F": dict[string],
    "avatar_F": dict[string],
    "nombre_G": dict[string],
    "avatar_G": dict[string]
    } status code : 200

    Ejemplo de salida correcta, la persona tiene 2 posiciones :
      {
    "ID": {
        "0": 3,
        "1": 3
    },
    "position": {
        "0": 2,
        "1": 9
    },
    "name": {
        "0": "Miguel",
        "1": "Miguel"
    },
    "email": {
        "0": "Miguel@gmail.com",
        "1": "Miguel@gmail.com"
    },
    "B": {
        "0": 5,
        "1": 2
    },
    "C": {
        "0": 5,
        "1": 2
    },
    "D": {
        "0": 6,
        "1": null
    },
    "E": {
        "0": 3,
        "1": null
    },
    "F": {
        "0": 5,
        "1": null
    },
    "G": {
        "0": 5,
        "1": null
    },
    "uplineID": {
        "0": 6,
        "1": 5
    },
    "avatar_A": {
        "0": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509",
        "1": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509"
    },
    "nombre_B": {
        "0": "Deya",
        "1": "Araceli"
    },
    "avatar_B": {
        "0": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509",
        "1": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509"
    },
    "nombre_C": {
        "0": "Deya",
        "1": "Araceli"
    },
    "avatar_C": {
        "0": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509",
        "1": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509"
    },
    "nombre_D": {
        "0": "Diego",
        "1": null
    },
    "avatar_D": {
        "0": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509",
        "1": null
    },
    "nombre_E": {
        "0": "Miguel",
        "1": null
    },
    "avatar_E": {
        "0": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509",
        "1": null
    },
    "nombre_F": {
        "0": "Deya",
        "1": null
    },
    "avatar_F": {
        "0": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509",
        "1": null
    },
    "nombre_G": {
        "0": "Deya",
        "1": null
    },
    "avatar_G": {
        "0": "https://gravatar.com/avatar/205e460b479e2e5b48aec07710c08d509",
        "1": null
    }
}

OUTPUT2:{
    "ID": {},
    "position": {},
    "name": {},
    "email": {},
    "B": {},
    "C": {},
    "D": {},
    "E": {},
    "F": {},
    "G": {},
    "uplineID": {},
    "avatar_A": {},
    "nombre_B": {},
    "avatar_B": {},
    "nombre_C": {},
    "avatar_C": {},
    "nombre_D": {},
    "avatar_D": {},
    "nombre_E": {},
    "avatar_E": {},
    "nombre_F": {},
    "avatar_F": {},
    "nombre_G": {},
    "avatar_G": {}
}, status code: 404 
    
OUTPUT3:{"message" : "algo salio mal en la consulta ", "log ": "string"} , status_code = 500 

OUTPUT4:{"message": "la peticion no tiene token"}, status code: 400 

OUTPUT5:{"mesage": "Invalid Token"} , status_code : 401

OUTPUT6:{"mesage": "Token Expired"} , status_code = 401 



    """
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

