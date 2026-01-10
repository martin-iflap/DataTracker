import data_tracker.db_manager as db
from typing import Tuple
import hashlib
import sqlite3
import os


def initialize_tracker() -> Tuple[bool, str]:
    """Initialize the .data_tracker directory and config.json file
    Returns: Tuple[bool, str]: (success, message)
    """
    existing_tracker = find_data_tracker_root()
    if existing_tracker:
        return False, f"Data tracker already initialized at {existing_tracker}"

    tracker_path = os.path.join(os.getcwd(), ".data_tracker")
    try:
        os.makedirs(os.path.join(tracker_path, "objects"))
        db_path = os.path.join(tracker_path, "tracker.db")

        success, message = db.initialize_database(db_path)
        return success, message

    except sqlite3.Error as e:
        return False, f"Database initialization error: {e}"
    except OSError as e:
        return False, f"Failed to create essential directories: {e}"

def add_data(data_path: str, title: str, version: int, notes: str) -> Tuple[bool, str]:
    """Add new data to be tracked into the .data_tracker/data directory
     - compute the hash of the file and use it as the unique identifier
     - copy file to the .data_tracker/objects directory and name it with its hash
     - fill in the database with dataset, object and version information
     - if no name is provided, generate a default name (dataset<num>)
    Returns: Tuple[bool, str]: (success, message)
    """
    data_path = os.path.abspath(data_path)

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

    db_path = os.path.join(tracker_path, "tracker.db")
    if not os.path.isfile(db_path):
        return False, "Data tracker database not found. Please run 'dt init' first."

    try:
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, title, notes)
            file_size = os.path.getsize(data_path)
            db.insert_object(conn, file_hash, file_size)
            db.insert_version(conn, dataset_id, file_hash, version, data_path)
            conn.commit()
    except sqlite3.Error as e:
        return False, f"Database error while adding data: {e}"
    # later add file deletion if database adding fails but file copied
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

def list_data() -> Tuple[bool, str]:
    """List all tracked data files in the data tracker.db datasets table
     - use the db_manager.py get_all_datasets function to retrieve datasets
     - format the output for display and return as a string
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = find_data_tracker_root() # check the find data tracker error handling
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        all_datasets = db.get_all_datasets(os.path.join(tracker_path, "tracker.db"))
        if not all_datasets:
            return True, "No datasets tracked yet."

        output_lines = ["Tracked Datasets:"]
        for dataset in all_datasets:
            output_lines.append(f"ID: {dataset['id']},  Name: {dataset['name']},  "
                                f"Created At: {dataset['created_at']},  Notes: {dataset['notes']}")

        return True, "\n".join(output_lines)
    except sqlite3.Error as e:
        return False, f"Database error while listing data: {e}"
    except OSError as e:
        return False, f"Filesystem error while listing data: {e}"

def get_history(data_id: int, name: str) -> Tuple[bool, str]:
    """Show history of versions with additional info for a specific data id or name"""
    try:
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        entire_history = db.get_dataset_history(os.path.join(tracker_path, "tracker.db"), data_id, name)
        if not entire_history:
            return True, "No history found for the specified dataset."

        output_lines = ["Dataset History:"]
        for record in entire_history:
            output_lines.append(
                f"Version: {record['version']}, ID: {record['id']}, Object Hash: {record['object_hash']}, "
                f"Original Path: {record['original_path']}, Added At: {record['created_at']},   Message: {record['message']}"
            )
        return True, "\n".join(output_lines)
    except sqlite3.Error as e:
        return False, f"Database error while retrieving history: {e}"
    except OSError as e:
        return False, f"Filesystem error while retrieving history: {e}"