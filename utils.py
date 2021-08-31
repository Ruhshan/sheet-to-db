# import psycopg2
from config import config
#from psycopg2 import sql, connect
import pymssql
import pandas as pd
from collections import Counter
from tkinter import Toplevel
import tkinter as tk

from connection_helper import ConnectionHelper
from db_handler import DbHandler
from enum import Enum

class Command(Enum):
    REPLACE = "REPLACE"
    APPEND = "APPEND"
    SKIP = "SKIP"



def get_columns_names(table):

    # declare an empty list for the column names
    columns = []
    params = config()
    conn = ConnectionHelper.getConnection()

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



def insert_to_db(df,connection, table_name, tk_root, dupeCheckFields):
    count = 0
    if check_table(table_name):
        records = df.to_dict('records')
        current_columns = list(df.columns)

        create_new_column_if_required(table_name, current_columns)
        for record in records:
            success = False
            has_duplicate, phys_locs = DbHandler.check_duplicate(record, table_name, dupeCheckFields)
            if has_duplicate:
                print("Found duplicate for record: ", record)
                for phys_loc in phys_locs:
                    command = check_prompt(record, table_name, phys_loc, tk_root)
                    if command == Command.REPLACE:
                        success = DbHandler.replace_at_phys_loc(record, table_name, phys_loc)
                    elif command == Command.APPEND:
                        success = DbHandler.create_new_row(record, table_name)
                    else:
                        print("Skipping", record)
            if not has_duplicate:
                success = DbHandler.create_new_row(record, table_name)
            if success:
                count+=1
        return count

    else:
        print('Creating new table')
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)
        return df.shape[0]


def check_table(table_name):
    sql_str = "select * from information_schema.tables where table_schema='dbo' and table_name='{}'".format(table_name)
    params = config()
    conn = ConnectionHelper.getConnection()
    cursor = conn.cursor()
    cursor.execute(sql_str)
    res = cursor.fetchall()

    return len(res)!=0

def create_new_column_if_required(table_name, current_df_columns):
    current_table_columns = get_columns_names(table_name)
    params = config()
    conn = ConnectionHelper.getConnection()
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


def check_prompt(row, table_name, phys_loc, root_tk):
    command = tk.IntVar()
    confirmation = Toplevel(root_tk)
    confirmation.geometry("800x100")
    confirmation.title("Duplicate found in DB")
    data_in_db = DbHandler.get_values_in_physloc(phys_loc, get_columns_names(table_name), table_name)
    label_data_in_sheet = tk.Label(confirmation, text="Data in sheet:")
    real_data_in_sheet = tk.Label(confirmation, text=str(row))
    label_data_in_sheet.grid(row=1,column=1)
    real_data_in_sheet.grid(row=1,column=2)
    label_data_in_db = tk.Label(confirmation, text="Data in db:")
    real_data_in_db = tk.Label(confirmation, text=data_in_db)
    label_data_in_db.grid(row=2, column=1)
    real_data_in_db.grid(row=2, column=2)

    button_replace = tk.Button(confirmation, text="Replace", command=lambda :command.set(1))
    button_append = tk.Button(confirmation, text="Append", command=lambda :command.set(2))
    button_skip = tk.Button(confirmation, text="Skip", command=lambda :command.set(3))

    button_replace.grid(row=3,column=1)
    button_append.grid(row=3, column=2)
    button_skip.grid(row=3, column=3)

    print("Waiting for confirmation")
    confirmation.waitvar(command)
    print("Received confirmation")
    confirmation.destroy()

    if command.get()==1:
        return Command.REPLACE
    elif command.get()==2:
        return Command.APPEND
    else:
        return Command.SKIP


def get_formatted_table_cols(list_of_cols):
    count = 0
    result_str=""
    for col in list_of_cols:
        if count == 7:
            result_str+=col+",\n"
            count=0
        else:
            result_str+=col+", "
            count+=1

    return result_str