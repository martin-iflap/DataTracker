import data_tracker.db_manager as db
from typing import Tuple
import hashlib
import sqlite3
import shutil
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
    try:
        data_path = os.path.abspath(data_path)

        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        file_hash = hash_file(data_path)
        if file_hash is None:
            return False, f"Failed to compute hash for {data_path}"

        _copy_file_to_objects(tracker_path, data_path, file_hash)

        db_path = os.path.join(tracker_path, "tracker.db")

        try:
            with db.open_database(db_path) as conn:
                dataset_id = db.insert_dataset(conn, title, notes)
                file_size = os.path.getsize(data_path)
                db.insert_object(conn, file_hash, file_size)
                db.insert_version(conn, dataset_id, file_hash, version, data_path)
                conn.commit()
        except sqlite3.Error as e:
            removed, err = _remove_file_object(tracker_path, file_hash)
            if not removed:
                return False, f"Failed to remove object file {file_hash}: {err} | After database error: {e}"
            return False, f"Database error while adding data: {e}"
    except FileNotFoundError:
        return False, f"Data path {data_path} does not exist"
    except OSError as e:
        return False, f"File operation failed: {e}"
    return True, f"Data at {data_path} added successfully"

def _copy_file_to_objects(tracker_path: str, data_path: str, file_hash: str) -> None:
    """Copy a file to the objects directory"""
    save_path = os.path.join(tracker_path, "objects", file_hash)
    if os.path.isfile(data_path):
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.copy2(data_path, save_path)
    else:
        raise OSError("Directory handling not implemented yet")

def hash_file(file_path: str) -> str | None:
    """Compute the hash of a file for versioning using SHA256"""
    if not os.path.isfile(file_path):
        return None
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def find_data_tracker_root(start_path: str = None) -> str | None:
    """Find the .data_tracker directory by searching upwards from the start_path
     - return the path if found or None if filesystem root is reached
    """
    if start_path is None:
        start_path = os.getcwd()

    try:
        current_path = os.path.abspath(start_path)

        while True:
            tracker_path = os.path.join(current_path, ".data_tracker")
            if os.path.exists(tracker_path):
                return tracker_path
            parent = os.path.dirname(current_path)
            if parent == current_path:
                return None
            current_path = parent
    except OSError:
        return None

def list_data() -> Tuple[bool, str]:
    """List all tracked data files in the data tracker.db datasets table
     - use the db_manager.py get_all_datasets function to retrieve datasets
     - format the output for display and return as a string
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        all_datasets = db.get_all_datasets(os.path.join(tracker_path, "tracker.db"))
        if not all_datasets:
            return True, "No datasets tracked yet."

        all_datasets = sorted(all_datasets, key=lambda x: x['id'])
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

def update_data(data_path: str, data_id: int, name: str, version: int, message: str) -> Tuple[bool, str]:
    """Add a new version of existing dataset to the tracker and tracker.db"""
    try:
        data_path = os.path.abspath(data_path)

        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        file_hash = hash_file(data_path)
        if file_hash is None:
            return False, f"Failed to compute hash for {data_path}"

        try:
            _copy_file_to_objects(tracker_path, data_path, file_hash)
        except OSError as e:
            return False, f"Failed to copy file to objects directory: {e}"

        with db.open_database(os.path.join(tracker_path, "tracker.db")) as conn:
            exists = db.dataset_exists(conn, data_id, name)
            if not exists:
                return False, "Specified dataset does not exist."

            if not data_id:
                data_id = db.get_id_from_name(conn, name)

            if version is None:
                version = db.get_next_version(conn, data_id)

            file_size = os.path.getsize(data_path)
            db.insert_object(conn, file_hash, file_size)
            db.insert_version(conn, data_id, file_hash, version, data_path, message)
            conn.commit()
        return True, f"Data at {data_path} updated successfully"
    except sqlite3.Error as e:
        return False, f"Database error while updating data: {e}"
    except OSError as e:
        return False, f"Filesystem error while updating data: {e}"

def remove_data(data_id: int, name: str) -> Tuple[bool, str]:
    """Remove data from the tracker by its ID or name
     - Delete object files from the objects table and return their hashes
     - Delete versions associated with the dataset
     - Delete the dataset entry from the datasets table
     - Remove object files from .data_tracker/objects using the returned hashes
    """
    hashes_to_remove = []
    try:
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        with db.open_database(os.path.join(tracker_path, "tracker.db")) as conn:
            exists = db.dataset_exists(conn, data_id, name)
            if not exists:
                return False, "Specified dataset does not exist."

            if data_id is None:
                data_id = db.get_id_from_name(conn, name)

            db.delete_versions(conn, data_id)
            hashes_to_remove = db.delete_object(conn, data_id)
            db.delete_dataset(conn, data_id)
            conn.commit()

        for file_hash in hashes_to_remove:
            removed, err = _remove_file_object(tracker_path, file_hash)
            if not removed:
                    return False, f"Failed to remove object file {file_hash}: {err}"

        return True, "Data removed successfully"
    except sqlite3.Error as e:
        return False, f"Database error while removing data: {e}"
    except OSError as e:
        return False, f"Filesystem error while removing data: {e}"

def _remove_file_object(tracker_path: str, file_hash: str) -> Tuple[bool, str]:
    """Delete the file object from the objects directory by its hash (name)
     - return True if successful, False otherwise
    """
    object_path = os.path.join(tracker_path, "objects", file_hash)
    try:
        os.remove(object_path)
    except FileNotFoundError:
        pass
    except OSError as e:
        return False, f"Failed to remove object file {object_path}: {e}"
    return True, ""
