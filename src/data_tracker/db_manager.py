from typing import Tuple
import sqlite3


def open_database(db_path: str) -> sqlite3.Connection:
    """Open a connection to the SQLite database with foreign key support
     - use it for all database operations for consistency and proper closing
     - use it as a context manager (built in to sqlite3.Connection)
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def initialize_database(db_path: str) -> Tuple[bool, str]:
    """Initialize the SQLite database with required tables"""
    with open_database(db_path) as conn:
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS datasets(
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         name TEXT UNIQUE
        )""")
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS objects(
                         hash TEXT PRIMARY KEY,
                         size INTEGER,
                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS versions(
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         dataset_id INTEGER NOT NULL,
                         object_hash TEXT NOT NULL,
                         version INTEGER NOT NULL,
                         original_path TEXT,
                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         FOREIGN KEY (dataset_id) REFERENCES datasets (id),
                         FOREIGN KEY (object_hash) REFERENCES objects (hash)
        )
        """)
        conn.commit()
    return True, "Data tracker initialized successfully"

def insert_dataset(conn: sqlite3.Connection, name: str) -> int:
    """Insert a new dataset into the dataset table of the tracker.db database"""
    cursor = conn.cursor()
    if name is None:
        cursor.execute("SELECT id FROM datasets ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        next_num = (row[0] + 1) if row else 1
        name = f"dataset-{next_num}"

    cursor.execute("INSERT INTO datasets (name) VALUES (?)", (name,))
    return cursor.lastrowid

def insert_object(conn: sqlite3.Connection, file_hash: str, size: int) -> None:
    """Insert a new object into the objects table of the tracker.db database"""
    conn.execute("INSERT OR IGNORE INTO objects (hash, size) VALUES (?, ?)",
                   (file_hash, size))

def insert_version(conn: sqlite3.Connection, data_set_id: int,
                   object_hash: str, version: int, data_path: str) -> None:
    """Insert a new version into the versions table of the tracker.db database"""
    conn.execute(
        "INSERT OR IGNORE INTO versions (dataset_id, object_hash, version, original_path) VALUES (?, ?, ?, ?)",
        (data_set_id, object_hash, version, data_path))

def get_all_datasets(db_path: str) -> list[sqlite3.Row]:
    """Retrieve all datasets from the datasets table of the tracker.db"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM datasets ORDER BY id ASC")
        rows = cursor.fetchall()
    return rows
