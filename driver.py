from tkinter import Tk

import pandas as pd
from config import config
from sqlalchemy import create_engine
from utils import insert_to_db

if __name__ == "__main__":
    stringed = b'\x14\x0b\x00\x01\x002\x00'.hex()

    converted_physloc = "0x" + stringed

    print(type(converted_physloc))

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




