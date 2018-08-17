class DB(object):
  def __init__(self, db_file=None):
    """
      Create a DB object.
      
      @param db_file Path to Database
    """
    self.conn = None
    self.db_file = db_file
    
  def connect(self, db_file=None):
    """ 
    Create a DB connection to SQLite DB.
    DB is created if it doesn't already exist.
    
    @return Connection object or None
    """
    pass
    
  def create_table(self, schema):
    """
    Execute create table statement
        
    @param schema CREATE TABLE statement
    
    @return Nothing
    """
    pass
    
  def query(self, query):
    """
    Execute query
    """
    pass
    
  def disconnect(self):
    """
    Close connection to DB
    """
    if self.conn is not None:
      self.conn.close()
      self.conn = None
