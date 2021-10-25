from config import config
from connection_helper import ConnectionHelper
import codecs


class DbHandler:
    auto_inc_col = None
    @classmethod
    def check_duplicate(cls, record: dict, tableName: str, dupeCheckFields: list):
        check_query_sql = cls.create_check_query(record, tableName, dupeCheckFields)
        conn = ConnectionHelper.getConnection()
        cursor = conn.cursor()
        cursor.execute(check_query_sql)
        rows = cursor.fetchall()
        if len(rows):
            return True, [row[-1] for row in rows]
        else:
            return False, None

    @classmethod
    def create_check_query(cls, row: dict, table, dupeCheckFields):
        params = config()
        query = "select *, {} as physloc from {}.dbo.{} where ".format(cls.auto_inc_col, params['database'], table)
        where_clauses = []
        for (key, value) in row.items():
            if key in dupeCheckFields:
                where_clauses.append(" [{}]='{}'".format(key, value))
        return query + " or ".join(where_clauses).replace("\'","'")

    @classmethod
    def create_new_row(cls, row: dict, table):
        print("Inserting ", row)
        params = config()
        query = f"INSERT INTO {params['database']}.dbo.{table} "
        keys = [f"[{key}]" for key in row.keys()]
        values = [f"N'{value}'" for value in row.values()]

        final_insert_query = query +f" ({', '.join(keys)}) VALUES ({', '.join(values)})"

        conn = ConnectionHelper.getConnection()
        try:
            cursor = conn.cursor()
            cursor.execute(final_insert_query)
            conn.commit()
            return True
        except Exception as e:
            print("Unable to insert row", row)
            print("Caught Exception", e)
        return False

    @classmethod
    def replace_at_phys_loc(cls, row, table, physLoc):
        print("________________________")
        print("Replacing", row, "at", physLoc)
        params = config()
        update_query = f"UPDATE {params['database']}.dbo.{table} SET "
        set_clauses = []
        for key,value in row.items():
            set_clauses.append(f"[{key}]=N'{value}'")

        update_query += ",".join(set_clauses) +F" WHERE {cls.auto_inc_col} = {physLoc}"

        conn = ConnectionHelper.getConnection()
        cursor = conn.cursor()
        try:
            print("Replacing with query:", update_query)
            cursor.execute(update_query)
            conn.commit()
            print("------------------------")
            return True
        except Exception as e:
            print("Unable to replace row ", row,"in ",physLoc)
            print("Caught Exception", e)
            print("------------------------")
        return False

    @classmethod
    def get_values_in_physloc(cls, physLoc, columns, table):
        params = config()
        select_query = f"SELECT * FROM {params['database']}.dbo.{table} WHERE  {cls.auto_inc_col} = {physLoc}"
        print("Getting values from:",physLoc)
        print("Generated Query:",select_query)

        conn = ConnectionHelper.getConnection()
        cursor = conn.cursor()
        cursor.execute(select_query)
        value_in_db = {}
        res = cursor.fetchall()
        if len(res)>0:
            for key,value in list(zip(columns, res[0])):
                value_in_db[key]=value

        return str(value_in_db)

    @classmethod
    def create_auto_inc_col(cls, table_name):
        params = config()
        sql = f"ALTER TABLE {params['database']}.dbo.{table_name} ADD auto_inc_col INT IDENTITY "
        print("No existing auto inc column found auto_inc_col is being created")
        conn = ConnectionHelper.getConnection()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cls.auto_inc_col = "auto_inc_col"


    @classmethod
    def set_aut_inc(cls, table, existing_columns):
        params = config()
        database = params["database"]
        for column in existing_columns:
            sql = f"select columnproperty(object_id('{database}.dbo.{table}'),'{column}','IsIdentity')"
            conn = ConnectionHelper.getConnection()
            cursor = conn.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()
            if res[0][0] == 1:
                cls.auto_inc_col = column
                print(f"Existing auto inc column {column} found.")
                break

        if cls.auto_inc_col is None:
            cls.create_auto_inc_col(table)



