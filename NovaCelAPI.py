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
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional


environ["GOOGLE_APPLICATION_CREDENTIALS"]="/keys_g/nova-cel-bot.json"
client = bigquery.Client()
sa= gspread.service_account(filename='/keys_g/sheetaccount.json')
sh =sa.open("Registro Nova cel (Responses)")

app = FastAPI()

# %%
#client.close()

# Registra la función no la modifica

class Registro(BaseModel):
    """Clase Eredada de registros para validación de parametros de entrada"""
    
    date_time : list [str] #fecha y hora en formato MM/DD/YYYY hh:mm:ss
    email : list [str] #correo
    name : list [str]# Nombre
    last_name : list [str]  #apellido
    age : list [int] #Edad
    country_lada : list [str]#lada del país 
    phone : list  [str]# telefono
    gender : list  [str] #genero
    url_avatar : list [str] # URL del avatar
    id : list [int] #id



@app.get("/")
def index():
    """Ruta principal"""
    print("Servidor OK")
    return {"mensaje": "Servidor OK ve a http://127.0.0.1:8000/docs"}




@app.post("/crea_nuevo_usuario")
def update_users( datos : Registro):
    
    
    """Recibe los datos en forma de dicionario, crea un dataframe en pandas , inserta los datos del data frame 
 

    """
    diccionario= ({
    'date_time' : datos.date_time, 
    'email' : datos.email, 
    'name' : datos.name, 
    'last_name' : datos.last_name,
    'age' : datos.age, 
    'country_lada' : datos.country_lada,
    'phone' : datos.phone, 
    'gender' : datos.gender, 
    'url_avatar' : datos.url_avatar, 
    'id' : datos.id 
    
    })

    
    print("Leyendo datos ... ")
    table_id = 'transact.users'
    
  
    df =  pd.DataFrame(data = diccionario)


    
    df['date_time']=pd.to_datetime(df['date_time'])
    df['age']=pd.to_numeric(df['age'])
    df['id']=pd.to_numeric(df['id'])
    df.phone = df.phone.astype(str)
    
    
    job_config = bigquery.LoadJobConfig(schema=[
    bigquery.SchemaField("date_time", "DATETIME"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("country_lada", "STRING"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("url_avatar", "STRING"),
    bigquery.SchemaField("id", "INTEGER",'REQUIRED')
    ])
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    
    job.result()
    
    if job.done() : 
        print ('Usuarios actualizados : {}'.format(len(df))) 
        return {"Estado": "OK"}
    else:
        print('Error algo salió mal ')
        return {"Estado": "Error algo salió mal"}
    
    return{"estado" : "pruena ok"}
    

# %%
#update_users() 

@app.post("/crea_estructura")
def insert_new_transact(ID=None):
    if ID==None:
        ID=input('Inserte el número de ID a insertar sin espacios, luego presione Enter ')
        ID=ID.replace(' ','')
        if ID.isdecimal():
            print('Buscando registro ...')
        else:
            print('Por favor inserte un número decimal ')
            return 1

    query2= """SELECT ID FROM `nova-cel.transact.transact` where ID={}""".format(ID)
    query_job = client.query(query2)  # Make an API request.
    df2=query_job.result().to_dataframe()  

    query3= """SELECT name,email FROM `nova-cel.transact.users` where ID={}""".format(ID)
    query_job = client.query(query3)  # Make an API request.
    df3=query_job.result().to_dataframe()

    if len(df2) ==0 and len(df3)==1 : #si no hay registro en transact pero sí existe ese id de usuairo
        print('insertando nuevo registro ...')
        query4="""INSERT `nova-cel.transact.transact` (ID,B,C,date_time_created,upline) \
        VALUES ({},NULL,NULL,CURRENT_DATETIME("America/Mexico_City"),NULL)""".format(ID)
        query_job = client.query(query4)
        query_job.result()  #Espera a que se ejeute el job
        #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
        if query_job.dml_stats.inserted_row_count==1:
            print('... Nuevo Registro Creado')
        else:
            print('El registro no se pudo crear ')

    elif len(df2)>0:
        print('.. El usuario ya está registrado, por favor termine su ciclo')
        #mostrar estadisticas
    elif len(df3)==0:
        print(' ... Ese ID de usuario no está registrado')
    elif len(df3)>1 :
        print('... Error: existen registros duplicados por ID !!!, favor de corregirlo')

# %%
#insert_new_transact(n)


def check_field_by_id(ID,field='name',table='transact'):
    
    query6="""SELECT ID, {} FROM `nova-cel.transact.{}` where ID={}""".format(field,table,ID)
    query_job = client.query(query6)  # Make an API request.
    df6=query_job.result().to_dataframe() 
    if len(df6)==0:
        print(' ... Ese ID de usuario no está registrado')   
        return None, False
    else:
        return df6[field].values[0], True
    

# %%
def set_upline(ID,ID2):
    query8="""UPDATE `nova-cel.transact.transact` SET upline={}, date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {}""".format(ID,ID2)
    query_job = client.query(query8)  # Make an API request.
    query_job.result()  #Espera a que se ejeute el job
    #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
    if query_job.dml_stats.updated_row_count==1:
        print('... upline Asociado')
        return 0
    else:
        print('No se pudo Asociar el upline de {}'.format(ID2))
        return 1

# %%
#check_field_by_id(27,'name',table='users')

@app.patch("/asociaar_registros")
def update_transact():
    """Asocia en el registro de ID el usuario ID2
    df5 es el registo ID
    df7 es el registro de su upline"""
    ID=input("Por favor inserte el ID de la persona a la cual desea asociar un miembro, luego precione Enter")
    query5="""SELECT ID, B,C, upline FROM `nova-cel.transact.transact` where ID={}""".format(ID)
    query_job = client.query(query5)  # Make an API request.
    df5=query_job.result().to_dataframe() 

    if df5.empty :
        print("El ID ingresado no tiene estructura favor de crearla")
        return 1
    
    if not df5.isnull()['upline'].values[0]: #si el registro actual no tiene upline por ejemplo ID=0 no se debe ejecutar
        query7="""SELECT ID, B,C, upline,D,E,F,G FROM `nova-cel.transact.transact` where ID={}""".format(df5['upline'].values[0])
        query_job = client.query(query7)  # Make an API request.
        df7=query_job.result().to_dataframe() #contiene los registros del upline, usamos solo D, E,F, G 

    if len(df5)==1:
        print('Registro encontrado en trasnsacciones')

        if df5['B'].isnull().values[0]== False and df5['C'].isnull().values[0]== False:
            print("El usuario {} ya tiene asociados a los usuarios {} y {} , termine su ciclo para iniciar uno nuevo".format(df5['ID'].values[0],df5['B'].values[0],df5['C'].values[0]))
            return 1
        
        if df5['upline'].isnull().values[0] == True and int(ID) > 0:
            print('Por favor primero asocie al usuario {} a su upline'.format(df5['ID'].values[0]))
            return 1
            
            

        ID2 =input("Inserte el ID del usuario que desea asociar debajo de {} ".format(df5['ID'].values[0]))
        
        if ID==ID2:
            print ("no se puede asociar un usuario debajo de si mismo")
            return 1


        name,result=check_field_by_id(ID2,'name',table='users')
        
        if result is not True :
            print("El usuario con el ID: {} no se encuentra registrado en el registro de usuarios".format(ID2))
            return 1
        
        uplineID,result2=check_field_by_id(ID2,'upline',table='transact')
        
        if result2 is not True:
            print("El usuario con el ID: {} no se encuentra registrado en el registro de estructura favor de agregarlo".format(ID2))
            return 1
        
        #if str(uplineID)!='nan' and str(uplineID)!='<NA>' :
        if str(uplineID)!='nan' and str(uplineID)!='<NA>' and str(uplineID)!='None' :
            print(uplineID)
            print("El usuario con el ID: {} ya se encuentra asociado al usuario {}".format(ID2,uplineID))
            return 1
        

        if df5['B'].isnull().values[0]== True:
            print("... asociando usuario en B")
            #Asociando en B

            query7="""UPDATE `nova-cel.transact.transact` SET B={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {}""".format(ID2,ID)
            query_job = client.query(query7)  # Make an API request.
            query_job.result()  #Espera a que se ejeute el job
            #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
            if query_job.dml_stats.updated_row_count==1:
                print('... Nuevo Usuario Asociado')
            else:
                print('El registro no se pudo crear ')
                return 1
            
            #acualizando en ID2 su ID de upline
            if set_upline(ID,ID2)==1:
                return 1
            
            if not df5.isnull()['upline'].values[0]: #si el registro actual no tiene upline por ejemplo ID=0 no se debe ejecutar

                
                
                if df7['C'].values[0]== int (ID) :
                    query9="""UPDATE `nova-cel.transact.transact` SET F={}, date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {}""".format(ID2,df5['upline'].values[0])
                    query_job = client.query(query9)  # Make an API request.
                    query_job.result()  #Espera a que se ejeute el job
                    #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
                    if query_job.dml_stats.updated_row_count==1:
                        print('... Registro asociado en F en upline')
                    else:
                        print("... Error, no se pudo actualizar el campo F en el registro {} ".format(ID))
                elif  df7['B'].values[0]== int(ID) :
                    query9="""UPDATE `nova-cel.transact.transact` SET D={}, date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {}""".format(ID2,df5['upline'].values[0])
                    query_job = client.query(query9)  # Make an API request.
                    query_job.result()  #Espera a que se ejeute el job
                    #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
                    if query_job.dml_stats.updated_row_count==1:
                        print('... Registro asociado en D en upline')
                    else:
                        print("... Error, no se pudo actualizar el campo D en el registro {} ".format(ID))


        elif df5['B'].isnull().values[0]== False and df5['C'].isnull().values[0]== True :
            print("... asociando usuario")

            if df5['B'].values[0]== int(ID2):
                print('El usuario ya se encuentra asociado')
                return 1
            #Asociando en C
            query7="""UPDATE `nova-cel.transact.transact` SET C={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {}""".format(ID2,ID)
            query_job = client.query(query7)  # Make an API request.
            query_job.result()  #Espera a que se ejeute el job
            #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
            if query_job.dml_stats.updated_row_count==1:
                print('... Nuevo Usuario Asociado en C')
            else:
                print('El registro no se pudo crear ')
                return 1
            
            #acualizando en ID2 su ID de upline
            if set_upline(ID,ID2)==1:
                return 1
            if not df5.isnull()['upline'].values[0]: #si el registro actual no tiene upline por ejemplo ID=0 no se debe ejecutar

                if df7['C'].values[0]== int (ID) :
                    query9="""UPDATE `nova-cel.transact.transact` SET G={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {}""".format(ID2,df5['upline'].values[0])
                    query_job = client.query(query9)  # Make an API request.
                    query_job.result()  #Espera a que se ejeute el job
                    #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
                    if query_job.dml_stats.updated_row_count==1:
                        print('... Registro asociado en G en upline')
                    else:
                        print("... Error, no se pudo actualizar el campo E en el registro {} ".format(ID))
                elif df7['B'].values[0]== int(ID) :
                    query9="""UPDATE `nova-cel.transact.transact` SET E={}, date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {}""".format(ID2,df5['upline'].values[0])
                    query_job = client.query(query9)  # Make an API request.
                    query_job.result()  #Espera a que se ejeute el job
                    #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
                    if query_job.dml_stats.updated_row_count==1:
                        print('... Registro asociado en E en upline')
                    else:
                        print("... Error, no se pudo actualizar el campo G en el registro {} ".format(ID))


    else:
        print('Usuario no encontrado en base de estructuras, favor de agregarlo')
        return 1

    return 0

# %%
#while True:
#update_transact()


@app.patch("/break_away")
def break_away(ID=None):
    if ID==None:
        ID=input('introduzca el ID del usuario para realizar Break Away: ')
    
    query7="""SELECT ID, B,C, upline,D,E,F,G FROM `nova-cel.transact.transact` where ID={}""".format(ID)
    query_job = client.query(query7)  # Make an API request.
    df7=query_job.result().to_dataframe() 
    #df7 contiene los registros del upline

    print ('validando estructura de usuario ...')
    B01 = not df7['B'].isnull().values[0]
    C01 = not df7['C'].isnull().values[0]
    D01 = not df7['D'].isnull().values[0]
    E01 = not df7['E'].isnull().values[0]
    F01 = not df7['F'].isnull().values[0]
    G01 = not df7['G'].isnull().values[0]
    
    if B01 and C01 and D01 and E01 and F01 and G01 :
        print(B01,C01,D01,E01,F01,G01)
        print('... ok')
    else:
        print('El usuario no está listo para hacer Break Away, favor de terminar estructura')
        return 1

    print('Generando respaldo de estructura ..')
    query10="""INSERT INTO  `nova-cel.transact.transact_hist`\
    (SELECT * from  `nova-cel.transact.transact` \
    where id={})""".format(ID)
    query_job = client.query(query10)  # Make an API request.
    query_job.result() #Espera a que se ejeute el job
    print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
    if query_job.dml_stats.inserted_row_count==1 :
        print('... ok')
    else:
        print('Error , algo salió mal')
        return 1


    query11="""UPDATE `nova-cel.transact.transact` SET upline=NULL, \
    date_time_created=CURRENT_DATETIME("America/Mexico_City") , date_time_updated=CURRENT_DATETIME("America/Mexico_City"), \
    B=NULL, C= NULL , D=NULL , E=NULL, F=NULL, G=NULL \
    where id={}""".format(ID)
    query_job = client.query(query11)  # Make an API request.
    query_job.result() #Espera a que se ejeute el job
    print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
    if query_job.dml_stats.updated_row_count==1:
        print('... Break Away del usuario Ok')
    else:
        print("... Error, no se pudo actualizar en el usuairo {} ".format(ID))
    

# %%
print('Bienvenido')
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
