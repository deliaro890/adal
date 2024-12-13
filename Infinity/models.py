
from pydantic import BaseModel 
from fastapi import Request 


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

class Usuario2(Request,BaseModel):
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

class Login_Google(BaseModel):
    """clase para el token de google"""
    token_google:str
    client_id : str

class Create_User_Google(BaseModel):
    token_google:str
    jwt : str
    client_id : str


class Ident(BaseModel,Request):
    """Clase para las funciones que solo requiren id """
    id : int

class Ident2(BaseModel):
    """Clase para las funciones que solo requiren id """
    id : int