import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from utils import *
from config import config
from sqlalchemy import create_engine
from tkinter import *

# Create object
root = Tk()
# Adjust size
root.geometry("600x750")

# get table and insert values
Label(root, text="Enter table Name:").pack(padx=5, pady=5)
tableName = Entry(root)
tableName.pack(padx=5, pady=5)
messageLabel = Label(root, text="")


def showColumns():
    tableCols = get_columns_names(tableName.get())
    messageLabel.config(text=", ".join(tableCols))
    print(tableCols)


showColumns = Button(root, text="Show Current Columns", command=showColumns).pack(padx=5, pady=5)

messageLabel.pack(padx=5, pady=5)

# accessing google APIs
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
# credentials to access files and responses
creds = ServiceAccountCredentials.from_json_keyfile_name("secrets.json", scope)
# connecting to a client
client = gspread.authorize(creds)

# save available gsheets in a list
response_sheets = dict()
# get all available sheets
for ind, sheet in enumerate(client.openall()):
    try:
        # check if this sheet is a response sheet
        if ('(Responses)' in sheet.title):
            response_sheets[sheet.title] = pd.DataFrame(sheet.get_worksheet(0).get_all_records())
    except Exception as e:
        print(e)

# get sheet names in array
available_sheets = list(response_sheets.keys())
# get form sheet name
sheetName = StringVar()
# Form select text
sheetName.set("Select a form")
# Create menu
drop = OptionMenu(root, sheetName, *available_sheets)
drop.pack(padx=5, pady=5)

df = None
checkStats = list()
insertButton = None


def refetch_sheet(sheet_name):
    for ind, sheet in enumerate(client.openall()):
        try:
            # check if this sheet is a response sheet
            if sheet.title == sheet_name:
                return pd.DataFrame(sheet.get_worksheet(0).get_all_records())
        except Exception as e:
            print(e)


def insertData():
    try:
        global checkStats
        df = get_new_rows(sheetName.get())
        if (df.shape[0]) == 0:
            return
        selectedFields = dict()
        for field in checkStats:
            if not field[0].get():
                continue
            else:
                if (field[-1].get() == "" or field[-1].get() == 'Enter new name:'):
                    selectedFields[field[1]] = field[1]
                else:
                    selectedFields[field[1]] = field[-1].get()
        if len(selectedFields) == 0:
            return
        df = df[list(selectedFields.keys())]
        df = df.rename(columns=selectedFields, inplace=False)
        params = config()
        engine = create_engine('mssql+pymssql://{user}:{password}@{host}:{port}/{db}'.format(
            user=params['user'],
            password=params['password'],
            host=params['host'],
            port=params['port'],
            db=params['database']
        ))
        df.to_csv
        conn = engine.connect()
        table = tableName.get()
        messageLabel.config(text="".format(df.shape[0]))
        if (table == ''):
            messageLabel.config(text="enter a valid table name")
            return
        # df.to_sql(table, con=conn, if_exists='replace', index=False)
        new_rows_count = insert_to_db(df, conn, table)
        messageLabel.config(text="Insert Successful! with {} rows".format(new_rows_count))
        mark_unmarked_rows(sheetName.get())
    except Exception as e:
        print(e)


def chooseFields():
    try:
        global sheetName
        global insertButton
        global messageLabel
        global df
        global checkStats
        sheet = sheetName.get()
        df = response_sheets[sheet]
        for checkStat in checkStats:
            checkStat[-2].destroy()
            checkStat[-1].destroy()
        if insertButton is not None:
            insertButton.destroy()
        checkStats = [[BooleanVar(), col] for col in list(df.columns)]

        for checkStat in checkStats:
            checkStat[0].set(False)
            checkStat.append(Checkbutton(root, text=checkStat[1], var=checkStat[0]))
            checkStat.append(Entry(root))
            checkStat[-2].pack(padx=50, anchor="w")
            checkStat[-1].insert(0, "Enter new name:")
            checkStat[-1].pack(padx=50, anchor="w", pady=1)
        insertButton = Button(root, text="Insert responses into select table", command=insertData)
        insertButton.pack(padx=5, pady=5)

    except Exception as e:
        print(e)


def get_new_rows(sheet_name):
    sh = client.open(sheet_name)
    worksheet = sh.get_worksheet(0)
    df = pd.DataFrame(worksheet.get_all_records())
    worksheet_list = [ws.title for ws in sh.worksheets()]
    if "Marker" not in worksheet_list:
        sh.add_worksheet(title="Marker", rows="4000", cols="1000")
        marker_sheet = sh.worksheet("Marker")
        marker_sheet.update('A1', 'Processed')
        return df
    else:
        marker = sh.worksheet("Marker")
        marker_df = pd.DataFrame(marker.get_all_records())
        concatenated = pd.concat([df, marker_df], axis=1)
        if "Processed" not in list(concatenated.columns):
            return df
        else:
            df = concatenated[concatenated["Processed"] != "TRUE"]
            return df.drop("Processed", axis=1)


def mark_unmarked_rows(sheet_name):
    sh = client.open(sheet_name)
    worksheet = sh.get_worksheet(0)
    df = pd.DataFrame(worksheet.get_all_records())
    marker_sheet = sh.worksheet("Marker")
    max_rows = df.shape[0]
    for i in range(2, max_rows + 2):
        marker_sheet.update("A{}".format(i), "TRUE")


# Create button, it will change show available response fields to choose from
chooseButton = Button(root, text="Choose response fields to insert", command=chooseFields).pack(padx=5, pady=5)

# Execute tkinter
root.mainloop()