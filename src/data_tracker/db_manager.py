from contextlib import contextmanager
from typing import Tuple
import sqlite3
import os


@contextmanager
def open_database(db_path: str):
    """Open a connection to the SQLite database with foreign key support.
    ACHTUNG! ATTENTION! ATENZIONE!
    IMPORTANT: You must call conn.commit() to persist changes!
    Yields:
        sqlite3.Connection: The database connection object.
    connection is automatically closed after use.
    """
    conn = sqlite3.connect(db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()

def initialize_database(db_path: str) -> Tuple[bool, str]:
    """Initialize the SQLite database with required tables"""
    with open_database(db_path) as conn:
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS datasets(
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         name TEXT UNIQUE,
                         message TEXT
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
                         FOREIGN KEY (dataset_id) REFERENCES datasets (id)
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS files(
            id INTEGER PRIMARY KEY,
            version_id INTEGER NOT NULL,
            object_hash TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            FOREIGN KEY(version_id) REFERENCES versions(id),
            FOREIGN KEY(object_hash) REFERENCES objects(hash)
        )""")
        conn.commit()
    return True, "Data tracker initialized successfully"

def insert_dataset(conn: sqlite3.Connection, name: str, message: str) -> int:
    """Insert a new dataset into the dataset table of the tracker.db database"""
    cursor = conn.cursor()

    cursor.execute("INSERT INTO datasets (name, message) VALUES (?, ?)", (name, message))
    id = cursor.lastrowid

    if name is None:
        name = f"dataset-{id}"
        cursor.execute("UPDATE datasets SET name = ? WHERE id = ?", (name, id))
    return id

def insert_object(conn: sqlite3.Connection, file_hash: str, size: int) -> None:
    """Insert a new object into the objects table of the tracker.db database"""
    conn.execute("INSERT OR IGNORE INTO objects (hash, size) VALUES (?, ?)",
                   (file_hash, size))

def insert_version(conn: sqlite3.Connection, data_set_id: int,
                   object_hash: str, version: int,
                   data_path: str, message: str = None) -> int:
    """Insert a new version into the versions table of the tracker.db database"""
    normalized_path = os.path.normpath(os.path.abspath(data_path))

    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO versions (dataset_id, object_hash, version, original_path, message) VALUES (?, ?, ?, ?, ?)",
        (data_set_id, object_hash, version, normalized_path, message))
    return cursor.lastrowid

def insert_files(conn: sqlite3.Connection, version_id: int, object_hash: str,
                 relative_path: str) -> None:
    """Insert a new file into the files table of the tracker.db database"""
    conn.execute(
        "INSERT INTO files (version_id, object_hash, relative_path) VALUES (?, ?, ?)",
        (version_id, object_hash, relative_path))

def get_all_datasets(db_path: str) -> list[dict]:
    """Retrieve all datasets from the datasets table of the tracker.db"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM datasets")
        return [dict(row) for row in cursor.fetchall()]

def get_dataset_history(db_path: str, dataset_id: int, name: str) -> list[dict]:
    """Retrieve all the version information for a specific dataset from the tracker.db version table"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()

        if dataset_id is None:
            dataset_id = get_id_from_name(conn, name)

        cursor.execute("""SELECT id, object_hash, version, original_path, message, created_at
                          FROM versions WHERE dataset_id = ? ORDER BY version""",
                       (dataset_id,))
        return [dict(row) for row in cursor.fetchall()]

def get_files_for_version(db_path, dataset_id: int, name: str, version: float) -> list[dict]:
    """Retrieve all files associated with a specific version ID from the tracker.db files table"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()
        if dataset_id is None:
            dataset_id = get_id_from_name(conn, name)

        cursor.execute("""
            SELECT relative_path, object_hash
            FROM files WHERE version_id = (
                SELECT id FROM versions
                WHERE dataset_id = ? AND version = ?
            )""", (dataset_id, version))
        return [dict(row) for row in cursor.fetchall()]

def get_id_from_name(conn: sqlite3.Connection, name: str) -> int:
    """Get the dataset ID from its name and return it"""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM datasets WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Dataset with name '{name}' does not exist.")
    return row['id']

def get_dataset_name_from_id(db_path: str, dataset_id: int) -> str:
    """Get the dataset name from its ID and return it"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM datasets WHERE id = ?", (dataset_id,))
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Dataset with ID '{dataset_id}' does not exist.")
        return row['name']

def dataset_exists(conn: sqlite3.Connection, dataset_id: int, name: str) -> bool:
    """Check if a dataset exists in the datasets tracker.db table by its ID or name"""
    cursor = conn.cursor()
    if dataset_id:
        cursor.execute("SELECT 1 FROM datasets WHERE id = ?", (dataset_id,))
    else:
        cursor.execute("SELECT 1 FROM datasets WHERE name = ?", (name,))
    return cursor.fetchone() is not None

def hash_exists(conn: sqlite3.Connection, file_hash: str) -> str | None:
    """Check if an object hash exists and if yes, return its version info in str else None"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM versions WHERE object_hash = ?", (file_hash,))
    row = cursor.fetchone()
    return (
        f"Version: {row['version']},  Original Path: {row['original_path']},  "
        f"Added At: {row['created_at']},  Message: {row['message']}"
    ) if row else None

def get_latest_version(conn: sqlite3.Connection, dataset_id: int) -> float:
    """Get the next version number for a dataset with a given ID"""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(version) FROM versions WHERE dataset_id = ?", (dataset_id,))
    result = cursor.fetchone()
    max_version = result[0] if result[0] is not None else 0
    return float(max_version)

def get_second_latest_version(conn: sqlite3.Connection, dataset_id: int) -> float | None:
    """Get the second-latest version number for a dataset with a given ID.
     - Returns None if fewer than 2 versions exist
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT version FROM versions WHERE dataset_id = ? ORDER BY version DESC LIMIT 2",
        (dataset_id,)
    )
    result = cursor.fetchall()
    if len(result) < 2:
        return None
    return float(result[1][0])

def get_object_size(db_path: str, object_hash: str) -> int:
    """Retrieve object size from the objects table of tracker.db by its hash"""
    with open_database(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT size FROM objects WHERE hash = ?", (object_hash,))
        row = cursor.fetchone()
        return row['size'] if row else 0

def delete_files(conn: sqlite3.Connection, dataset_id: int) -> None:
    """Delete all files associated with a dataset from the files table of tracker.db"""
    conn.execute("DELETE FROM files WHERE version_id IN (SELECT id FROM versions WHERE dataset_id = ?)", (dataset_id,))

def delete_object(conn: sqlite3.Connection) -> list[str]:
    """Delete an object from the objects table of the tracker.db database
     - Delete only if no other versions reference the same object_hash\
     - Return a list of deleted object hashes for further file system cleanup
    """
    hash_list = []

    rows = conn.execute("""
                        SELECT hash FROM objects
                        WHERE hash NOT IN (SELECT DISTINCT object_hash FROM files)
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

def object_is_used(conn: sqlite3.Connection, object_hash: str) -> bool:
    """Check if an object hash is referenced by any version in the versions table"""
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM versions WHERE object_hash = ?", (object_hash,))
    return cursor.fetchone() is not None

def find_dataset_by_path(db_path: str, path: str) -> int | None:
    """Find dataset by matching original_path"""
    abs_path = os.path.abspath(path)

    with open_database(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT dataset_id
            FROM versions
            WHERE original_path = ?
            ORDER BY version DESC
            LIMIT 1
        """, (abs_path,))

        row = cursor.fetchone()
        return row['dataset_id'] if row else None

def check_version_exists(conn: sqlite3.Connection, dataset_id: int, version: float) -> bool:
    """Check if a specific version exists for a dataset in the versions table
     - Return True if version exists, else False
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM versions WHERE dataset_id = ? AND version = ?",
        (dataset_id, version)
    )
    return cursor.fetchone() is not None

def update_dataset_name(conn: sqlite3.Connection, dataset_id: int, new_name: str) -> None:
    """Rename a dataset in the datasets table of the tracker.db database"""
    conn.execute("UPDATE datasets SET name = ? WHERE id = ?", (new_name, dataset_id))

def update_version_message(conn: sqlite3.Connection, dataset_id: int, version: float, new_message: str) -> int:
    """Update the message of a specific version for a dataset in the versions table of the tracker.db database
     - Return the number of rows updated (should be 1 if successful, 0 if no matching version found)
    """
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE versions SET message = ? WHERE dataset_id = ? AND version = ?",
        (new_message, dataset_id, version)
    )
    return cursor.rowcount

def update_dataset_message(conn: sqlite3.Connection, dataset_id: int, new_message: str) -> int:
    """Update the message of a dataset in the datasets table of the tracker.db database
     - Return the number of rows updated (should be 1 if successful, 0 if no matching dataset found)
    """
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE datasets SET message = ? WHERE id = ?",
        (new_message, dataset_id)
    )
    return cursor.rowcount