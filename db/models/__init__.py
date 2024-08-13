import os
import sqlite3

def get_db_connection():
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), '../../data/preservation.db'))
    conn.row_factory = sqlite3.Row
    return conn
