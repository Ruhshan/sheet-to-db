import pymssql
from config import config


class ConnectionHelper:
    conn = None

    @classmethod
    def getConnection(cls):
        params = config()
        if cls.conn is None:
            # if conn is none, create a connection and return it
            cls.conn = pymssql.connect(
                database=params['database'],
                user=params['user'],
                server=params['host'] + ":" + params["port"],
                password=params['password']
            )
            return cls.conn
        else:
            # if conn is not none return it
            return cls.conn
