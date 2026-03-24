import sqlite3
import os
from pathlib import Path

# This finds the 'O2C-GRAPH-SYSTEM' folder automatically
BASE_DIR = Path(__file__).resolve().parent.parent
# This forces the path to be /opt/render/project/src/data/o2c.db on the cloud
DEFAULT_DB = str(BASE_DIR / "data" / "o2c.db")

DB_PATH = os.environ.get("DB_PATH", DEFAULT_DB)

def get_connection():
    # Ensure the data directory exists before connecting
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn