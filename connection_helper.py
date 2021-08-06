from sqlalchemy.dialects.mssql import pymssql
from config import config


class ConnectionHelper:
    conn = None

    @classmethod
    def getConnection(cls):
        params = config()
        if cls.conn is None:
            cls.conn = pymssql.connect(
                database=params['database'],
                user=params['user'],
                server=params['host'] + ":" + params["port"],
                password=params['password']
            )
            return cls.conn
        else:
            return cls.conn
