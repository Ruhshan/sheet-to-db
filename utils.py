# import psycopg2
from config import config
#from psycopg2 import sql, connect
import pymssql
import pandas as pd
from collections import Counter

def get_columns_names(table):

    # declare an empty list for the column names
    columns = []
    params = config()
    conn = pymssql.connect(
        database = params['database'],
        user = params['user'],
        server = params['host']+":"+params["port"],
        password = params['password']
    )

    # declare cursor objects from the connection
    col_cursor = conn.cursor()

    # concatenate string for query to get column names
    # SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'some_table';
    col_names_str = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE "
    col_names_str += "table_name = '{}';".format( table )

    try:
        # sql_object = sql.SQL(
        #     # pass SQL statement to sql.SQL() method
        #     col_names_str
        # ).format(
        #     # pass the identifier to the Identifier() method
        #     sql.Identifier( table )
        # )

        # execute the SQL string to get list with col names in a tuple
        col_cursor.execute( col_names_str )

        # get the tuple element from the liast
        col_names = ( col_cursor.fetchall() )

        # iterate list of tuples and grab first element
        for tup in col_names:

            # append the col name string to the list
            columns += [ tup[0] ]

        # close the cursor object to prevent memory leaks
        col_cursor.close()

    except Exception as err:
        print ("get_columns_names ERROR:", err)

    # return the list of column names
    return columns


def insert_to_db(df,connection, table_name):
    if check_table(table_name):
        print("Appeding to existing table")
        create_new_column_if_required(table_name, df.columns)
        df.to_sql(table_name, con=connection, if_exists='append', index=False)
        return df.shape[0]
    else:
        print('Creating new table')
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)
        return df.shape[0]


def check_table(table_name):
    sql_str = "select * from information_schema.tables where table_schema='dbo' and table_name='{}'".format(table_name)
    params = config()
    conn = pymssql.connect(
        database = params['database'],
        user = params['user'],
        server = params['host']+":"+params["port"],
        password = params['password']
    )
    cursor = conn.cursor()
    cursor.execute(sql_str)
    res = cursor.fetchall()

    return len(res)!=0

def create_new_column_if_required(table_name, current_df_columns):
    current_table_columns = get_columns_names(table_name)
    params = config()
    conn = pymssql.connect(
        database = params['database'],
        user = params['user'],
        server = params['host']+":"+params["port"],
        password = params['password']
    )

    for df_column in current_df_columns:
        if df_column not in current_table_columns:
            print("Add {} in {}".format(df_column, table_name))
            sql = 'ALTER TABLE "{}" ADD "{}" varchar(max);'.format(table_name, df_column)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
