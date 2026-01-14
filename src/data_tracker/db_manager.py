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
                         name TEXT UNIQUE,
                         notes TEXT
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
                         message TEXT,
                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         FOREIGN KEY (dataset_id) REFERENCES datasets (id),
                         FOREIGN KEY (object_hash) REFERENCES objects (hash)
        )
        """)
        conn.commit()
    return True, "Data tracker initialized successfully"

def insert_dataset(conn: sqlite3.Connection, name: str, notes: str) -> int:
    """Insert a new dataset into the dataset table of the tracker.db database"""
    cursor = conn.cursor()
    if name is None:
        cursor.execute("SELECT id FROM datasets ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        next_num = (row[0] + 1) if row else 1
        name = f"dataset-{next_num}"

    cursor.execute("INSERT INTO datasets (name, notes) VALUES (?, ?)", (name, notes))
    return cursor.lastrowid

def insert_object(conn: sqlite3.Connection, file_hash: str, size: int) -> None:
    """Insert a new object into the objects table of the tracker.db database"""
    conn.execute("INSERT OR IGNORE INTO objects (hash, size) VALUES (?, ?)",
                   (file_hash, size))

def insert_version(conn: sqlite3.Connection, data_set_id: int,
                   object_hash: str, version: int,
                   data_path: str, message: str = None) -> None:
    """Insert a new version into the versions table of the tracker.db database"""
    conn.execute(
        "INSERT OR IGNORE INTO versions (dataset_id, object_hash, version, original_path, message) VALUES (?, ?, ?, ?, ?)",
        (data_set_id, object_hash, version, data_path, message))

def get_all_datasets(db_path: str) -> list[sqlite3.Row]:
    """Retrieve all datasets from the datasets table of the tracker.db"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM datasets")
        return cursor.fetchall()

def get_dataset_history(db_path: str, dataset_id: int, name: str) -> list[sqlite3.Row]:
    """Retrieve all the version information for a specific dataset from the tracker.db version table"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()

        if not dataset_id:
            dataset_id = get_id_from_name(conn, name)

        cursor.execute("""SELECT id, object_hash, version, original_path, message, created_at
                          FROM versions WHERE dataset_id = ? ORDER BY version""",
                       (dataset_id,))
        return cursor.fetchall()

def get_id_from_name(conn: sqlite3.Connection, name: str) -> int:
    """Get the dataset ID from its name and return it"""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM datasets WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Dataset with name '{name}' does not exist.")
    return row['id']

def dataset_exists(conn: sqlite3.Connection, dataset_id: int, name: str) -> bool:
    """Check if a dataset exists in the datasets tracker.db table by its ID or name"""
    cursor = conn.cursor()
    if dataset_id:
        cursor.execute("SELECT 1 FROM datasets WHERE id = ?", (dataset_id,))
    else:
        cursor.execute("SELECT 1 FROM datasets WHERE name = ?", (name,))
    return cursor.fetchone() is not None

def get_next_version(conn: sqlite3.Connection, dataset_id: int) -> int:
    """Get the next version number for a dataset with a given ID"""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(version) FROM versions WHERE dataset_id = ?", (dataset_id,))
    result = cursor.fetchone()
    max_version = result[0] if result[0] is not None else 0
    return max_version + 1

def delete_object(conn: sqlite3.Connection, dataset_id: int) -> list[str]:
    """Delete an object from the objects table of the tracker.db database
     - Delete only if no other versions reference the same object_hash\
     - Return a list of deleted object hashes for further file system cleanup
    """
    hash_list = []

    rows = conn.execute("""
                        SELECT hash FROM objects
                        WHERE hash NOT IN (SELECT DISTINCT object_hash FROM versions)
                        """).fetchall()
    for row in rows:
        object_hash = row['hash']
        hash_list.append(object_hash)
        conn.execute("DELETE FROM objects WHERE hash = ?", (object_hash,))
    return hash_list

def delete_versions(conn: sqlite3.Connection, dataset_id: int) -> None:
    """Delete all versions associated with a dataset from the versions table of the tracker.db database"""
    conn.execute("DELETE FROM versions WHERE dataset_id = ?", (dataset_id,))

def delete_dataset(conn: sqlite3.Connection, dataset_id: int) -> None:
    """Delete a dataset from the datasets table of the tracker.db database"""
    conn.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))
