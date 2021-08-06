# import psycopg2
from config import config
#from psycopg2 import sql, connect
import pymssql
import pandas as pd
from collections import Counter
from tkinter import Toplevel
import tkinter as tk

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


# def insert_to_db(df,connection, table_name, tk_root, dupeCheckFields):
#     #take_confirmation(root_tk=tk_root)
#     if check_table(table_name):
#         print("Appeding to existing table")
#         create_new_column_if_required(table_name, df.columns)
#         df.to_sql(table_name, con=connection, if_exists='append', index=False)
#         return df.shape[0]
#     else:
#         print('Creating new table')
#         df.to_sql(table_name, con=connection, if_exists='replace', index=False)
#         return df.shape[0]

def insert_to_db(df,connection, table_name, tk_root, dupeCheckFields):
    if check_table(table_name):
        records = df.to_dict('records')
        ## iterate over each row
        for record in records:
            check_query = create_check_query(record, table_name, dupeCheckFields)
            print(check_query)
        ### Create query to see if the row with selected fields for dupe check exists in db
        ## if not insert it
        ## if yes show prompt
        print("ok")
    else:
        print("insert all")


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

def take_confirmation(root_tk):
    confirmation = Toplevel(root_tk)
    confirmation.geometry("200x300")
    label_data_in_sheet = tk.Label(confirmation, text="Data in sheet:")
    real_data_in_sheet = tk.Label(confirmation, text="a=b;c=d;e=f")
    label_data_in_sheet.grid(row=1,column=1)
    real_data_in_sheet.grid(row=1,column=2)
    label_data_in_db = tk.Label(confirmation, text="Data in db:")
    real_data_in_db = tk.Label(confirmation, text="a=b;c=d;e=f")
    label_data_in_db.grid(row=2, column=1)
    real_data_in_db.grid(row=2, column=2)

    button_replace = tk.Button(confirmation, text="Replace")
    button_append = tk.Button(confirmation, text="Append")
    button_skip = tk.Button(confirmation, text="Skip")

    button_replace.grid(row=3,column=1)
    button_append.grid(row=3, column=2)
    button_skip.grid(row=3, column=3)

    # my_str1 = tk.StringVar()
    # l1 = tk.Label(confirmation, textvariable=my_str1)
    # l1.grid(row=1, column=2)
    # my_str1.set("Hi I am Child window")
    # b2 = tk.Button(confirmation, text=' Close parent',)
    # b2.grid(row=2, column=2)
    #
    # b3 = tk.Button(confirmation, text=' Close Child',
    #                command=confirmation.destroy)
    # b3.grid(row=3, column=2)

def create_check_query(row:dict, table, dupeCheckFields):
    params = config()
    query = "select * from '{}.dbo.{}' where ".format(params['database'], table)
    where_clauses = []
    for (key,value) in row.items():
        if key in dupeCheckFields:
            where_clauses.append(" {}='{}'".format(key, value))
    return query+ " and ".join(where_clauses)
