import os

DEFAULT_DB_NAME="bsedata.db"
DEFAULT_DB_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_DB = DEFAULT_DB_PATH+'/'+DEFAULT_DB_NAME
