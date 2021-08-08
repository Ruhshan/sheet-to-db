from config import config
from connection_helper import ConnectionHelper

class DbHandler:
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
        query = "select *, %%physloc%% as physloc from {}.dbo.{} where ".format(params['database'], table)
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
        converted_physloc = cls.convert_phyloc(physLoc)
        print("Replacing", row, "at", converted_physloc)
        params = config()
        update_query = f"UPDATE {params['database']}.dbo.{table} SET "
        set_clauses = []
        for key,value in row.items():
            set_clauses.append(f"[{key}]=N'{value}'")

        update_query += ",".join(set_clauses) +F" WHERE %%physloc%% = {converted_physloc}"

        conn = ConnectionHelper.getConnection()
        cursor = conn.cursor()
        try:
            cursor.execute(update_query)
            conn.commit()
            return True
        except Exception as e:
            print("Unable to replace row ", row,"in ",converted_physloc)
            print("Caught Exception", e)
        return False

    @classmethod
    def convert_phyloc(cls, physLoc):
        stringed = str(physLoc)
        converted_physloc = "0x" + stringed.replace('\\x', '').replace('b', '').replace("'", "")\
            .replace("\\a","07")\
            .replace("\\b","08")\
            .replace("\\t","09")\
            .replace('\\n','0A')\
            .replace('\\v','0B')\
            .replace('\\f','0C')\
            .replace("\\r","0D")
        return converted_physloc

    @classmethod
    def get_values_in_physloc(cls, physLoc, columns, table):
        params = config()
        converted_physloc = cls.convert_phyloc(physLoc)
        select_query = f"SELECT * FROM {params['database']}.dbo.{table} WHERE  %%physloc%% = {converted_physloc}"
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





