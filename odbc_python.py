import pyodbc 
import env
import os
import pandas as pd
import psycopg2

username = os.environ['USERNAME']  
password = os.environ['USER_PWD'] #set a enviroment path to keep your pwd

def conecta_odbc():
    username = os.environ['USERNAME']  
    password = os.environ['USER_PWD']
    server = 'server.net' 
    database = 'database_name'     
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
    
    

def conecta_oracle():    
    try:
        con = cx_Oracle.connect('user','pwd','con:1521/xe')
        cursor = con.cursor()
        return(con)
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


def select_oracle(con, columns, table, exception):
    try:
        if len(exception.replace(' ','')) < 1: 
            select = 'SELECT '+columns+' from '+table
        else:
            select = 'SELECT '+columns+' from '+table+ ' where '+exception
        
        table = pd.read_sql(select,con)
        return(table)
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)



def insert_oracle(con, df, table):
    col = df.columns.tolist()
    print(col)
    vrs = ""
    for i in range(0,len(col)):
        vrs += ':'+str(i+1)+','
    sql = 'INSERT INTO '+table+' VALUES ('+vrs.rstrip(',')+')'
    print(sql)
    df = df.fillna('')
    j = 0
    while j < len(df):
        cols = []
        for i in range(j,len(df)):
            cols.append(tuple(df.values[i]))
            print(i)
            print(tuple(df.values[i]))
            j +=1
            if len(cols) == 50000:
                j +=1
                break                               
        cursor =  con.cursor()
        cursor.executemany(sql,cols,batcherrors=True)
        for error in cursor.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        con.commit()

def update_database(con, df, table):
    col = df.columns.tolist()
    vrs = ""
    for i in range(len(col)):
        vrs += col[i] + '=:' + str(i + 1) + ','
    sql = f'UPDATE {table} SET {vrs.rstrip(",")} WHERE num_conta = :{len(col) + 1}'
    df = df.fillna('')
    j = 0
    while j < len(df):
        cols = []
        for i in range(j, len(df)):
            cols.append(tuple(df.values[i]))
            j += 1
            if len(cols) == 50000:
                j += 1
                break
        cursor = con.cursor()
        cursor.executemany(sql, cols, batcherrors=True)
        for error in cursor.getbatcherrors():
            print("Error", error.message, "at row offset", error.offset)
        con.commit()
