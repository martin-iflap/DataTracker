from typing import Tuple
import hashlib
import sqlite3
import os

# add original file path to database?
def initialize_tracker() -> Tuple[bool, str]:
    """Initialize the .data_tracker directory and config.json file
    Returns: Tuple[bool, str]: (success, message)
    """
    tracker_path = os.path.join(os.getcwd(), ".data_tracker")
    if os.path.exists(tracker_path):
        return False, "Data tracker already initialized"
    try:
        os.makedirs(os.path.join(tracker_path, "objects"))

        db_path = os.path.join(tracker_path, "tracker.db")
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("""
                CREATE TABLE IF NOT EXISTS datasets(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    name TEXT UNIQUE
                );
                CREATE TABLE IF NOT EXISTS objects (
                    hash TEXT PRIMARY KEY,
                    size INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id INTEGER NOT NULL,
                    object_hash TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
                    FOREIGN KEY (object_hash) REFERENCES objects(hash)
                );
                """)

            return True, "Data tracker initialized successfully"
        except sqlite3.Error as e:
            raise Exception(f"Database initialization error: {e}")
    except OSError as e:
        raise Exception(f"Failed to initialize data tracker: {e}")

def add_data(data_path: str, dataset_name: str, version: int) -> Tuple[bool, str]:
    """Add new data to be tracked into the .data_tracker/data directory
     - compute the hash of the file and use it as the unique identifier
     - copy file to the .data_tracker/objects directory and name it with its hash
     - fill in the database with dataset, object and version information
     - if no name is provided, generate a default name (dataset<num>)
    Returns: Tuple[bool, str]: (success, message)
    """
    if not os.path.exists(data_path):
        return False, f"Data path {data_path} does not exist"

    tracker_path = find_data_tracker_root()
    if tracker_path is None:
        return False, "Data tracker is not initialized. Please run 'dt init' first."

    file_hash = hash_file(data_path)
    if file_hash is None:
        return False, f"Failed to compute hash for {data_path}"

    try:
        save_path = os.path.join(tracker_path, "objects", file_hash)
        if os.path.isdir(data_path):
            pass # add later
        else:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(data_path, "rb") as src_file:
                with open(save_path, "wb") as dest_file:
                    dest_file.write(src_file.read())
    except OSError as e:
        return False, f"Failed to add data: {e}"

    try:
        db_path = os.path.join(tracker_path, "tracker.db")
        if not os.path.isfile(db_path):
            return False, "Data tracker database not found. Please run 'dt init' first."
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            if dataset_name is None:
                cursor.execute("SELECT id FROM datasets ORDER BY id DESC LIMIT 1")
                row = cursor.fetchone()
                next_num = (row[0] + 1) if row else 1
                dataset_name = f"dataset-{next_num}"

            cursor.execute("INSERT INTO datasets (name) VALUES (?)", (dataset_name,))
            data_set_id = cursor.lastrowid

            file_size = os.path.getsize(data_path)
            cursor.execute("INSERT OR IGNORE INTO objects (hash, size) VALUES (?, ?)",
                           (file_hash, file_size))

            cursor.execute("INSERT OR IGNORE INTO versions (dataset_id, object_hash, version) VALUES (?, ?, ?)",
                           (data_set_id, file_hash, version))
    except sqlite3.Error as e:
        return False, f"Database error while adding data: {e}"

    return True, f"Data at {data_path} added successfully"

def hash_file(file_path: str) -> str | None:
    """Compute the hash of a file for versioning using SHA256"""
    if not os.path.isfile(file_path):
        return None
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except OSError:
        return None

def find_data_tracker_root(start_path: str = None) -> str | None:
    """Find the .data_tracker directory by searching upwards from the start_path
     - return the path if found or None if filesystem root is reached
    """
    if start_path is None:
        start_path = os.getcwd()

    current_path = os.path.abspath(start_path)

    while True:
        tracker_path = os.path.join(current_path, ".data_tracker")
        if os.path.exists(tracker_path):
            return tracker_path
        parent = os.path.dirname(current_path)
        if parent == current_path:
            return None
        current_path = parent

