from pydantic import BaseModel
import pandas  as pd
from google.cloud import bigquery
import json
import traceback
import re
from random import randint
import correo
from fastapi.responses import JSONResponse
from functions_jwt import write_token

table_id_users= "nova-cel.loyallty.users"
table_id_codes_email = "nova-cel.loyallty.codes_email"
table_id_transact = "nova-cel.loyallty.transact"
table_id_transact_user_view = "nova-cel.loyallty.transact_user_view"

class Usuario(BaseModel):
    """Clase Eredada de registros para validación de parametros de entrada"""
    
    #date_time_created :  str #fecha y hora en formato MM/DD/YYYY hh:mm:ss
    email : str #correo
    name : str# Nombre
    last_name : str  #apellido
    age : int #Edad
    country_lada : str#lada del país 
    phone : str# telefono
    gender : str #genero
    url_avatar : str # URL del avatar
    #id : int #id
    password : str


def create_user (diccionario : dict , client : bigquery.client.Client ) :
    "Create a new user not use date_time_creation neither id "
    #df = pd.DataFrame(columns=['date_time_creation','email','name','last_name','age','country_lada','phone','gender','url_avatar','id','password'])


    print (diccionario)

    if not validate_email (diccionario['email']):
        return JSONResponse (content = {"message": "correo invalido"} , status_code = 400 )

    if validate_exist(diccionario['email'], client) :
        return JSONResponse (content = {"message": "correo {} ya registrado" .format (email) } , status_code = 409 )
        

    id = max_id_actual(client) + 1

    query1= ("""INSERT `{}` 
    (date_time_created,email,name,last_name,age,country_lada,phone,gender,url_avatar,id,password,paid_positions) \
    VALUES  (CURRENT_DATETIME("America/Mexico_City"), "{}","{}","{}",{},"{}","{}","{}","{}",{},"{}", 0 ) """
             .format(table_id_users, diccionario['email'], diccionario['name'] , diccionario['last_name'] , 
                     str (diccionario['age']) , diccionario['country_lada'] , diccionario['phone'] , diccionario['gender'] ,  
                     diccionario['url_avatar'] , id , diccionario['password']) )

    print (query1)

    try:
        query_job = client.query(query1)  # Make an API request
        query_job.result() #espera a que termine 
        if query_job.done() :
            return {"code": 200 , "message" : "New User Created with id : {}" .format (id) }
    except Exception :
        return {"code" : 500 , "message" : "Something wrong ", "log ": traceback.format_exc()}
        

def max_id_actual (client : bigquery.client.Client) :
    """Regresa un entero ,el maximo id actual """
    query2 = """SELECT MAX (id ) as max_id FROM `nova-cel.loyallty.users` """
    try:
        query_job = client.query(query2)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df2 = query_job.to_dataframe()
            if str (df2['max_id'][0]) == 'nan' or str (df2['max_id'][0])  == '<NA>' or str (df2['max_id'][0])  == 'None':   #no no hay usuarios en la tabla 
                 return 0
            else :
                return df2.values[0][0]  #si hay usuarios regresa el id del útimo 
    except :
        return {"code" : 500 , "message" : "algo salio mal en max_id_actual", "log ": traceback.format_exc()}


def validate_exist (correo : str ,client : bigquery.client.Client):
    """Regresa True si existe , regresa False si No existe """
    query3 = """SELECT * FROM `{}` where email = "{}" """.format(table_id_users,correo)

    #print(query3)


    query_job = client.query(query3)  # Make an API request
    try:
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df2 = query_job.to_dataframe()
            if len(df2)==0:   #si no hay usuarios/registros en la tabla 
                 return False
            else :
                return True  #El usuairo ya existe
    except :
        raise #no se pudo completar el job

def validate_email (email : str):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    if (re.fullmatch(regex, email)) :
        return True
    else:
        return False

def add_pay_position (email : str , client : bigquery.client.Client ) :
    """ Añade un pago """

    if not validate_email (email):
        return JSONResponse (content = {"message": "correo invalido"} , status_code = 400 )

    if not validate_exist(email, client) :
        return JSONResponse (content = {"message": "correo {} no encontrado" .format (email) } , status_code = 404 )
    
    query3 = """SELECT paid_positions  FROM `{}` where email = "{}" """.format(table_id_users,email)
    print(query3)

    query_job = client.query(query3)  # Make an API request

    try:
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df3 = query_job.to_dataframe()
            print(df3)
            print (type(df3.values[0][0]))
            print (df3.values[0][0])
            paids = df3.values[0][0] + 1 #Añade un pago

            query33 = """ UPDATE `{}`  SET paid_positions = {} where email ="{}" """. format (table_id_users, paids, email)
            print (query33)
            query_job33 = client.query(query33)  # Make an API request
            query_job33.result() #espera a que termine el job
            if query_job33.done() :
                return JSONResponse (content = {"message": "pago de posición agregado a cliente", "log": str(query_job33.dml_stats) } , status_code = 200 )

        else :
            return JSONResponse (content = {"message": "Something wrong ", "log ": "consulta no terminada "} , status_code = 500 )
    except :
        return JSONResponse (content = {"message": "Something wrong ", "log ": traceback.format_exc()} , status_code = 500 )

def update_user(diccionario : dict , client : bigquery.client.Client):
    """Actualiza los datos de un usuario se debe de enviar el dicionario completo """

    if not validate_exist(diccionario['email'], client) :
        return JSONResponse (content = {"message": "correo {} no existe" .format (email) } , status_code = 409 )

    query4= """ UPDATE `{}`  SET name = "{}", last_name = "{}", age = {} ,country_lada = "{}", phone = "{}", gender = "{}" , url_avatar = "{}", password = "{}" \
                where email ="{}" """.format(table_id_users,  diccionario['name'] , diccionario['last_name'] , 
                     str (diccionario['age']) , diccionario['country_lada'] , diccionario['phone'] , diccionario['gender'] ,  
                     diccionario['url_avatar'] , diccionario['password'] , diccionario['email'] )
    print (query4)
    
    query_job = client.query(query4)  # Make an API request
    try:
        query_job.result() #espera a que termine el job
        if query_job.done() :
            return {"code" : 200, "message" : "usuario actualizado"  , "log" : str(query_job.dml_stats) }
    
        else :
           return {"code" : 500, "message" : "no se pudo actualizar" }  #si no pudo terminar la consulta
    except :
        return {"code" : 500 , "message" : "algo salio mal ", "log ": traceback.format_exc()}

def login_user ( email : str, password : str, client : bigquery.client.Client ):
    
    if not validate_email (email):
        return JSONResponse (content = {"message": "correo invalido"} , status_code = 400 )
        #return {"code": 500 , "message" : "correo invalido" }

    if not validate_exist(email, client) :
        return JSONResponse (content = {"message": "correo {} no encontrado" .format (email) } , status_code = 404 )

    query5 = """ SELECT * from `{}` where email = "{}" and password = "{}" """.format(table_id_users,email, password)

    try:
        query_job = client.query(query5)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df2 = query_job.to_dataframe()
            if len(df2)==0:   #si no hay usuarios/registros en la tabla
                return JSONResponse (content = {"message" : "contraseña invalida" } , status_code = 401)
            else :
                output = df2.to_dict()
                token = write_token ( {"email" : email , "password" : password } )
                output["Authorization"] = token
                return  output
    except :
        return JSONResponse (content = {"message" : "algo salio mal en max_id_actual" , "log" : traceback.format_exc() } , status_code = 500)


def send_code ( email : str  , client : bigquery.client.Client) :
    try:

        if not validate_email (email)  :
            return {"code" : 500 , "message" : " Correo invalido"}

        code = randint(100000,999999)
        correo.send_email(email,"Tu código de verificación es : {}" .format(code))
    
        query6= (""" INSERT  `{}`  (email , code , date_time_created ) \
        VALUES ( "{}" , {} , CURRENT_DATETIME("America/Mexico_City")) """.format(table_id_codes_email, email , code ) )
        print (query6)
        query_job = client.query(query6)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            return JSONResponse (content =  {"message" : "Código enviado por correo y guardado en base" , "log" : str(query_job.dml_stats) }, status_code = 200)
        else :
            return JSONResponse (content =  {"message" : "no se pudo guardar el código en la Base de datos" }, status_code = 500)
    except :
        return JSONResponse (content =  {"message" : "algo salio mal ", "log ": traceback.format_exc() }, status_code = 500)

def verify_code (correo : str ,code : int , client : bigquery.client.Client):
    """Regresa True si es el mismo código , regresa False si No existe """
    query7 = """SELECT code FROM `{}` where email = "{}"  ORDER BY date_time_created DESC LIMIT 1 """.format(table_id_codes_email,correo)

    print(query7)
    
    try:
        query_job = client.query(query7)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df7 = query_job.to_dataframe()
            if len(df7)==0:   #si no hay usuarios/registros en la tabla 
                 return {"code" : 500, "message" : "No hay código para ese correo"  }
            print ("codigo es: " + str(df7.values[0][0]))
            if df7.values[0][0] == code:
                return JSONResponse (content =  {"message" : "Sí coincide el código" }, status_code = 200)
            else :
                return JSONResponse (content =  {"message" : "No coincide el código" }, status_code = 406)
                
    except :
        return JSONResponse (content =  {"message" : "something wrong " , "log" :  traceback.format_exc()  }, status_code = 406)

def get_id_and_position (client):
    """Obtiene el numero de id al cual se le debe asociar alguien debajo, así como su posicion, regresa un entero  """
    query = """ SELECT id , position FROM `{}` where b is null or c is null  order by position asc limit 1 """.format (table_id_transact)
    try:
        
        query_job = client.query(query)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df = query_job.to_dataframe()
            if df.empty :
                print ("no hay registros todavia en transacciones")
                return 0
            else :
                return df['id'].values[0] , df['position'].values[0]               
    except :
        print ("El job no se pudo completar" )
        print (traceback.format_exc())
        raise

def get_next_position(client):
    """ Regresa la POSICION int siguiente que se debe insertar, ya considera el primer caso """
    query = """ select count (date_time_created ) from `{}`  """.format(table_id_transact) 

    try:
        
        query_job = client.query(query)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df = query_job.to_dataframe()
            return df.values[0][0] + 1                
    except :
        print ("El job no se pudo completar" )
        print (traceback.format_exc())
        raise


def insert_new_transact(ID2 : int , client : bigquery.client.Client):
    """Importante Usar Solo si el id de usuario ya pagó , genera la estructura lógica del usuario en la tabla de transacciones 
    ID2 es el usuario que entra
    ID es el usuario que ya está dentro y se le asocia debajo el usuario que entra"""



    query3= """SELECT name,email FROM `{}` where ID={}""".format(table_id_users,ID2)
    query_job = client.query(query3)  # Make an API request.
    df3=query_job.result().to_dataframe()

    if len(df3)==1 : #por no dejar verifica que existe ese id de usuairo 

        if not check_paids_positions (ID2,client) : # y verifica que haya pagado su posición !
            return JSONResponse (content =  {"message" : "hay que pagar la posición primero" }, status_code = 402)

        position_id2 = get_next_position(client)
        
        print('insertando nuevo registro ...')
        query4="""INSERT `{}` (ID,B,C,date_time_created,upline, position) \
        VALUES ({},NULL,NULL,CURRENT_DATETIME("America/Mexico_City"),NULL, {} )""".format(table_id_transact,ID2, position_id2 )
        query_job = client.query(query4)
        query_job.result()  #Espera a que se ejeute el job
        #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
        if query_job.dml_stats.inserted_row_count==1:
            print('... Nuevo Registro Creado')
        else:
            print('El registro no se pudo crear ')


    elif len(df3)==0:
        print(' ... Ese ID de usuario no está registrado')
    elif len(df3)>1 :
        print('... Error: existen registros duplicados por ID !!!, favor de corregirlo')
        return JSONResponse (content =  {"message" : "Error: existen registros duplicados por ID !!!, favor de corregirlo "} , status_code = 409)


    ID , position_id = get_id_and_position(client)  #Este id es al que se le tiene que asociar este nuevo usuario que se integra 

    #print (str(position_id))

    if position_id2 == 0 :
        return JSONResponse (content =  {"message" : "El primer registro no necesita estár asociado a nadie felicidades por empezar"} , status_code = 200)

    print ("posicion entrante : {} \n id al que debe de asignarse id2: {} \n posicion al que debe de agregarse el id entrante {} ".format (position_id2, ID, str(position_id) ))
    
    return update_transact(ID, ID2, position_id,position_id2,client )

def check_field_by_id_user(ID= None,field='name',table='users', client = None):

    """Regresa el valor del campo buscado y un True si el registro existe,  regresa un None y False si no existe ,  """
    
    query6="""SELECT ID, {} FROM `{}` where ID={} """.format(field,table,ID)
    query_job = client.query(query6)  # Make an API request.
    df6=query_job.result().to_dataframe() 
    if len(df6)==0:
        print(' ... Ese ID de usuario no está registrado')   
        return None, False
    else:
        return df6[field].values[0], True


def check_field_by_id_transact(ID = None ,field='upline',table='transact' , position = 0 , client = None):

    """Regresa el valor del campo buscado y un True si el registro existe,  regresa un None y False si no existe ,  """
    
    query6="""SELECT ID, {} FROM `{}` where ID={} and position = {} """.format(field,table,ID, position )
    query_job = client.query(query6)  # Make an API request.
    df6=query_job.result().to_dataframe() 
    if len(df6)==0:
        print(' ... Ese ID de usuario no está registrado')   
        return None, False
    else:
        return df6[field].values[0], True


def check_current_position_by_id(ID = None , table = 'transact', client = None):

    """Regresa el valor del campo buscado y un True si el registro existe,  regresa un None y False si no existe ,  """
    
    query6="""SELECT position as position FROM `{}` where ID={} and (D is null or E is null or F is null or G is null) order by position limit 1 """.format(table,ID )
    query_job = client.query(query6)  # Make an API request.
    #print (query6)
    df6=query_job.result().to_dataframe()
    
    #print (df6)
    if len(df6)==0:
        print(' ... Ese ID de usuario no está registrado')   
        return None, False
    else:
        return df6['position'].values[0], True


def set_upline(ID,ID2,position_id2,client):

    """ID: usuario upline
    ID2 : usuario nuevo 
    position : la posición del nuevo usuario (ID2)"""

    
    query8="""UPDATE `{}` SET upline={}, date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {} and position = {} """.format(table_id_transact,ID,ID2, position_id2)
    query_job = client.query(query8)  # Make an API request.
    query_job.result()  #Espera a que se ejeute el job
    #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
    if query_job.dml_stats.updated_row_count==1:
        print('... upline Asociado en el registro actual')
        return 0
    else:
        print('No se pudo Asociar el upline de {}'.format(ID2))
        return 1

def update_transact(ID, ID2, position_id,position_id2,client ):
    """Asocia en el registro de ID el usuario ID2
    df5 es el registo ID (es el upline de ID2)
    df7 es el registro ID2 (el que va entrando )
    ID2 es el usuario que entra
    ID es el usuario que ya está dentro y se le asocia debajo el usuario que entra
    df8 es el registro del upline de ID 
    uplineID es el upline de ID"""
    
    #ID=input("Por favor inserte el ID de la persona a la cual desea asociar un miembro, luego precione Enter")
    query5="""SELECT ID, B,C, upline FROM `{}` where ID={} and position = {}""".format(table_id_transact,ID, position_id)
    query_job = client.query(query5)  # Make an API request.
    df5=query_job.result().to_dataframe() 

    if df5.empty :
        print("El ID ingresado no tiene estructura favor de crearla")
        return {"code" : 500 , "message" : "El ID ingresado no tiene estructura favor de crearla"}
    
    query7="""SELECT ID, B,C, upline FROM `{}` where ID={} AND position = {} """.format(table_id_transact, ID2 ,position_id2)
    query_job = client.query(query7)  # Make an API request.
    df7=query_job.result().to_dataframe() #contiene los registros del upline de ID, usamos solo B y C


    if len(df5) == 1:
        print('Registro encontrado en trasnsacciones')

        if df5['B'].isnull().values[0]== False and df5['C'].isnull().values[0]== False:
            print("El usuario {} ya tiene asociados a los usuarios {} y {} , termine su ciclo para iniciar uno nuevo".format(df5['ID'].values[0],df5['B'].values[0],df5['C'].values[0]))
            return {"code" : 500 , "message" : "El usuario {} ya tiene asociados a los usuarios {} y {} , termine su ciclo para iniciar uno nuevo".format(df5['ID'].values[0],df5['B'].values[0],df5['C'].values[0]) }
        
        if df5['upline'].isnull().values[0] == True and position_id > 1:
            print('Por favor primero asocie al usuario {} a su upline'.format(df5['ID'].values[0]))
            return {"code" : 500 , "message" : 'Por favor primero asocie al usuario {} a su upline'.format(df5['ID'].values[0])}
            
            

        #ID2 =input("Inserte el ID del usuario que desea asociar debajo de {} ".format(df5['ID'].values[0]))
        
        if ID==ID2:
            print ("precaución, está asociando un usuario debajo de si mismo")
        


        name,result=check_field_by_id_user(ID2,'name',table_id_users,client)
        
        if result is not True :
            print("Error , El usuario con el ID: {} no se encuentra registrado en el registro de usuarios".format(ID2))
            return {"code":500 , "message" : "Error , El usuario con el ID: {} no se encuentra registrado en el registro de usuarios".format(ID2) }


        if position_id2 > 3 :
            uplineID,result2=check_field_by_id_transact(ID,'upline',table_id_transact , position_id , client)
            if result2 is not True:
                print("Error El usuario con el ID: {} no se encuentra registrado en el registro de estructura favor de agregarlo".format(ID2))
                return {"code":500 , "message" : "Error El usuario con el ID: {} no se encuentra registrado en el registro de estructura favor de agregarlo".format(ID2) }

            position_uplineID,result3 = check_current_position_by_id (uplineID,table_id_transact, client)
        

            if str(uplineID)!='nan' and str(uplineID)!='<NA>' and str(uplineID)!='None' :
                print(uplineID)
                print("El usuario con el ID: {} se encuentra debajo del usuario {} que tiene la posicion {} ".format(ID,uplineID,position_uplineID))

            query8="""SELECT ID, D,E,F,G  FROM `{}` where ID={} and position = {} """.format(table_id_transact, uplineID ,position_uplineID)
            query_job = client.query(query8)  # Make an API request.
            df8=query_job.result().to_dataframe() #contiene los registros del upline de ID, usamos solo D , E , F y G

        if position_id2 == 1 :
            print ("ok Done")
            return {"code ":200 , "message": "ok Done" }
            

        if df5['B'].isnull().values[0]== True  and df5['C'].isnull().values[0]== True:
            
            print("... asociando usuario en B")
            #Asociando en B

            query7="""UPDATE `{}` SET B={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {} and position = {} """.format(table_id_transact,ID2,ID, position_id)
            query_job = client.query(query7)  # Make an API request.
            query_job.result()  #Espera a que se ejeute el job
            #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
            if query_job.dml_stats.updated_row_count==1:
                print('... Nuevo Usuario Asociado en B')
            else:
                print('El usuario no se pudo asociar ')
                return {"code ":500 , "message": 'El usuario no se pudo asociar ' }

        if df5['B'].isnull().values[0]== False  and df5['C'].isnull().values[0]== True:
            print("... asociando usuario en C")
            #Asociando en C

            query7="""UPDATE `{}` SET C={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {} and position = {} """.format(table_id_transact,ID2,ID, position_id)
            query_job = client.query(query7)  # Make an API request.
            query_job.result()  #Espera a que se ejeute el job
            #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
            if query_job.dml_stats.updated_row_count==1:
                print('... Nuevo Usuario Asociado en C')
            else:
                print('El usuario no se pudo asociar ')
                return {"code ":500 , "message": 'El usuario no se pudo asociar ' }

            
            #acualizando en ID2 su ID de upline
        if set_upline(ID,ID2, position_id2,client)==1:
            return {"code ":500 , "message": 'El usuario no se pudo asociar ' }

        if position_id2 <= 3 :
            print ("ok Done")
            return {"code ":200 , "message": "ok Done" }


        if df8['D'].isnull().values[0]== True and df8['E'].isnull().values[0]== True and df8['F'].isnull().values[0]== True and df8['G'].isnull().values[0]== True:
            
            query7="""UPDATE `{}` SET D={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {} and position = {} """.format(table_id_transact,ID2,uplineID, position_uplineID)
            query_job = client.query(query7)  # Make an API request.
            query_job.result()  #Espera a que se ejeute el job
            #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
            if query_job.dml_stats.updated_row_count==1:
                print('... Nuevo Usuario Asociado en D')
            else:
                print('El usuario no se pudo asociar ')
                return {"code ":500 , "message": 'El usuario no se pudo asociar ' }

        elif df8['D'].isnull().values[0]== False and df8['E'].isnull().values[0]== True and df8['F'].isnull().values[0]== True and df8['G'].isnull().values[0]== True :
            
            query7="""UPDATE `{}` SET E={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {} and position = {} """.format(table_id_transact,ID2,uplineID, position_uplineID)
            query_job = client.query(query7)  # Make an API request.
            query_job.result()  #Espera a que se ejeute el job
            #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
            if query_job.dml_stats.updated_row_count==1:
                print('... Nuevo Usuario Asociado en E')
            else:
                print('El usuario no se pudo asociar ')
                return {"code ":500 , "message": 'El usuario no se pudo asociar ' }

        elif df8['D'].isnull().values[0]== False and df8['E'].isnull().values[0]== False and df8['F'].isnull().values[0]== True and df8['G'].isnull().values[0]== True :
            query7="""UPDATE `{}` SET F={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {} and position = {} """.format(table_id_transact,ID2,uplineID, position_uplineID)
            query_job = client.query(query7)  # Make an API request.
            query_job.result()  #Espera a que se ejeute el job
            #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
            if query_job.dml_stats.updated_row_count==1:
                print('... Nuevo Usuario Asociado en F')
            else:
                print('El usuario no se pudo asociar ')
                return {"code ":500 , "message": 'El usuario no se pudo asociar ' }

        elif df8['D'].isnull().values[0]== False and df8['E'].isnull().values[0]== False and df8['F'].isnull().values[0]== False and df8['G'].isnull().values[0]== True :
             
             query7="""UPDATE `{}` SET G={} , date_time_updated=CURRENT_DATETIME("America/Mexico_City") where ID = {} and position = {} """.format(table_id_transact,ID2,uplineID, position_uplineID)
             query_job = client.query(query7)  # Make an API request.
             query_job.result()  #Espera a que se ejeute el job
             #print(query_job.dml_stats)  # Muestra detalle de filas insertadas, eliminadas o actualizadas
             if query_job.dml_stats.updated_row_count==1:
                 print('... Nuevo Usuario Asociado en G')
             else:
                 print('El usuario no se pudo asociar ')
                 return {"code ":500 , "message": 'El usuario no se pudo asociar ' }
        



    elif len(df5)> 1 :
        print ("parece que hay más de un registro con el mismo id en la misma posicion , revise la base de datos !!!")
        return {"code ":500 , "message": "parece que hay más de un registro con el mismo id en la misma posicion , revise la base de datos!!!"}
    else:
        print('Usuario no encontrado en base de estructuras, favor de agregarlo')
        return {"code ":500 , "message": 'Usuario no encontrado en base de estructuras, favor de agregarlo'}
    
    print ("Ok Done")
    return  {"code ":200 , "message": "Usuario asociado correctamente" }
 
    
def check_paids_positions (ID,client):
    """ si hay mas posiciones pagadas de las que se están ocupando regresa un True, de lo contrario regresa un false"""
    Query = """select sum (paid_positions) as paid_positions from `{}` where id = {}""".format (table_id_users, ID )
    try:
        query_job = client.query(Query)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            
            df = query_job.to_dataframe()
            posiciones_pagadas = df['paid_positions'].values[0]
            print ("posiciones pagadas: {}".format(posiciones_pagadas) )
            try:
                query2 = """select count (id) as count_id from `{}` where id = {}""".format (table_id_transact, ID )
                query_job = client.query(query2)  # Make an API request
                query_job.result() #espera a que termine el job
                if query_job.done() :
                    df = query_job.to_dataframe()
                    posiciones_actuales = df['count_id'].values[0]
                    print ("posiciones acutales: {}".format(posiciones_actuales) )

                    if posiciones_pagadas > posiciones_actuales :
                        return True 
                    else :
                        print ("se requiere pagar la posición")
                        return False 

            
            except :
                print ("El job para revisar las posiciones actuales usadas no se pudo completar" )
                print (traceback.format_exc())
                return False             
    except :
        print ("El job para revisar las posiciones pagadas no se pudo completar" )
        print (traceback.format_exc())
        return False 

def info_user_by_id ( id : int , client : bigquery.client.Client ):
    """Regresa todos los registros del id a buscar en la vista de transacciones y usuarios , limitado a los ultimos 100 registros"""
    
    query5 = """ SELECT * from `{}` where id = {} order by position asc limit 100  """.format(table_id_transact_user_view,str(id))
    #print (query5)

    try:
        query_job = client.query(query5)  # Make an API request
        query_job.result() #espera a que termine el job
        if query_job.done() :
            df2 = query_job.to_dataframe()
            if len(df2)==0:   #si no hay usuarios/registros en la tabla 
                 return   JSONResponse (content = df2.to_dict() , status_code = 404 ) 
            else :
                return JSONResponse (content = df2.to_dict() , status_code = 200)   #si hay usuarios regresa el id del útimo 
    except :
        return JSONResponse (content = {"message" : "algo salio mal en la consulta ", "log ": traceback.format_exc()} , status_code = 500)
         

    

    
        
        