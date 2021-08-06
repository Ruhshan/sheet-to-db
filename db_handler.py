from config import config
from connection_helper import ConnectionHelper

class DbHandler:
    @classmethod
    def check_duplicate(cls, record: dict, tableName: str, dupeCheckFields: list):
        check_query_sql = cls.create_check_query(record, tableName, dupeCheckFields)
        conn = ConnectionHelper.getConnection()
        cursor = conn.cursor()
        cursor.execute(check_query_sql)
        res = cursor.fetchall()
        if len(res):
            return True, res[0][-1]
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
        return query + " and ".join(where_clauses).replace("\'","'")

    @classmethod
    def create_new_row(cls, row: dict, table):
        params = config()
        query = f"INSERT INTO {params['database']}.dbo.{table} "
        keys = [f"[{key}]" for key in row.keys()]
        values = [f"N'{value}'" for value in row.values()]

        final_insert_query = query +f" ({', '.join(keys)}) VALUES ({', '.join(values)})"

        conn = ConnectionHelper.getConnection()
        cursor = conn.cursor()
        cursor.execute(final_insert_query)
        conn.commit()



