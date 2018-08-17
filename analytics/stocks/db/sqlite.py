import sqlite3
from sqlite3 import Error

from db.defaults import DEFAULT_DB
from db.base import DB

class Sqlite(DB):

    def __init__(self, db_file=DEFAULT_DB):
        """
        Create a DB object.
        
        @param db_file Path to Database
        """
        super(Sqlite, self).__init__(db_file)
        
    def connect(self, ):
        """ 
        Create a DB connection to SQLite DB.
        DB is created if it doesn't already exist.
        
        @return Connection object or None
        """
        try:
            self.conn = sqlite3.connect(self.db_file)
            return True
        except Error as e:
            print(e)
        return False

    def create_table(self, schema):
        """
        Execute create table statement
        
        @param schema CREATE TABLE statement
        
        @return Nothing
        """

        try:
            if self.conn is None:
                raise Exception('DB not connected')
            c = self.conn.cursor()
            c.execute(schema)
        except Error as e:
            print (e)
            
    def query(self, table, values):
        """
        Insert
        
        @param schema CREATE TABLE statement
        
        @return Nothing
        """

        try:
            if self.conn is None:
                raise Exception('DB not connected')
            c = self.conn.cursor()
            c.execute(query)
        except Error as e:
            print (e)

    def create_row(self, table, values):
        sql = 'INSERT INTO '+table+'('
        sql += ','.join([key for key in values])
        sql += ') VALUES('+','.join(['?' for i in range(len(keys))])+')'
        
        db_vals = (values[key] for key in values)
        cur = conn.cursor()
        cur.execute(sql, db_vals)
        
        return cur.lastrowid


