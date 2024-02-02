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
    try:
        cursor =  con.cursor()
        col = df.columns.tolist()
        vrs = ""
        for i in range(0,len(col)):
            vrs += ':'+str(i+1)+','
        cols = []
        df = df.fillna('')
        sql = 'INSERT INTO '+table+' VALUES ('+vrs.rstrip(',')+')'
        for i in range(0,len(df)):
            cols.append(tuple(df.values[i]))
            print(i)        
        cursor.executemany(sql,cols)
        con.commit()
    except cx_Oracle.Error as error:
        print('Error occurred: '+error)


def update_database():
    try:
        # Establish a connection to the database
        connection = psycopg2.connect(
            database="database",
            user="user",
            password="password",
            host="host",
            port="5432"
        )

        # Create a cursor object
        cursor = connection.cursor()

        # SQL query to update the table
        update_query = """UPDATE table SET column = value WHERE condition"""
        query = """SELECT * FROM nome_da_tabela"""
        
        cursor.execute(update_query)
        # Commit the changes made to the database
        connection.commit()
        print("Database successfully updated")
        return query

    except (Exception, psycopg2.Error) as error:
        print("Error while updating PostgreSQL table", error)
    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
)
