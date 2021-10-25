from tkinter import Tk

import pandas as pd
from config import config
from sqlalchemy import create_engine
from utils import insert_to_db

from db_handler import DbHandler

if __name__ == "__main__":
    DbHandler.set_aut_inc("mytable",['Shirt size', 'Other thoughts or comments', 'auto_inc_col', 'Timestamp', 'uid'])
    # root = Tk()
    # # Adjust size
    # root.geometry("600x750")
    #
    # data = {'Timestamp': {7: '8/4/2021 13:10:07', 8: '8/4/2021 13:50:10'},
    #         'Name': {7: 'John', 8: 'Dave'},
    #         'Shirt size': {7: 'XL', 8: 'M'},
    #         'Other thoughts or comments': {7: 'Chole ar ki', 8: 'New comment for replace'}}
    #
    # df = pd.DataFrame.from_dict(data)
    # selectedFieldsForDupeCheck = ["Name","Shirt size"]
    # params = config()
    # engine = create_engine('mssql+pymssql://{user}:{password}@{host}:{port}/{db}'.format(
    #     user=params['user'],
    #     password=params['password'],
    #     host=params['host'],
    #     port=params['port'],
    #     db=params['database']
    # ))
    #
    # conn = engine.connect()
    # table = "mytable"
    # insert_to_db(df, conn, table, root, selectedFieldsForDupeCheck)
    # root.mainloop()




