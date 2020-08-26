from db.sqlite import Sqlite

INDUSTRY_TABLE = """ CREATE TABLE IF NOT EXISTS industry (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL); """
                    
SCRIPS_TABLE = """ CREATE TABLE IF NOT EXISTS scrips (
                    id INTEGER PRIMARY KEY,
                    sid TEXT NOT NULL,
                    name TEXT NOT NULL,
                    equity_group TEXT NOT NULL,
                    face_value INTEGER NOT NULL,
                    isin TEXT NOT NULL,
                    industry INTEGER,
                    FOREIGN KEY (industry) REFERENCES industry (id)); """
                    
DATA_TABLE = """ CREATE TABLE IF NOT EXISTS data (
                    id INTEGER PRIMARY KEY,
                    stock INTEGER NOT NULL,
                    date REAL NOT NULL,
                    day_open REAL NOT NULL,
                    day_high REAL NOT NULL,
                    day_low REAL NOT NULL,
                    day_close REAL NOT NULL,
                    wap REAL NOT NULL,
                    shares_traded INTEGER NOT NULL,
                    num_trades INTEGER NOT NULL,
                    turnover REAL NOT NULL,
                    deliverable INTEGER NOT NULL,
                    percent REAL NOT NULL,
                    spread_hl REAL NOT NULL,
                    spread_co REAL NOT NULL,
                    FOREIGN KEY (stock) REFERENCES scrips (id)); """
db = Sqlite()
if db.connect() is True:
    db.create_table(INDUSTRY_TABLE)
    db.create_table(SCRIPS_TABLE)
    db.create_table(DATA_TABLE)
else:
    print('Error occurred while connecting to DB')
    
db.disconnect()
