import pyodbc 
import env
import os
import pandas as pd

username = os.environ['USERNAME']  
password = os.environ['USER_PWD'] #set a enviroment path to keep your pwd

def conecta_odbc():
    username = os.environ['USERNAME']  
    password = os.environ['USER_PWD']
    server = 'server.net' 
    database = 'ldw'     
    con = pyodbc.connect('DRIVER={driver_name};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return con


def select(con, columns, table, condit):
    if len(condit.replace(' ','')) < 1:
        select = 'SELECT '+columns+' from '+table
    else:
        select = 'SELECT '+columns+' from '+table+ ' where '+condit
    
    table = pd.read_sql(select,con)
    df = pd.DataFrame(table)

    return df
    
    
