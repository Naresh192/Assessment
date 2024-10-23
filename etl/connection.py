import sqlite3

def connect_to_db(db_name) :
    conn = sqlite3.connect(db_name)
    return conn

