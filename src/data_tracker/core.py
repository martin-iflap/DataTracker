import data_tracker.db_manager as db
from typing import Tuple
import subprocess
import tempfile
import sqlite3
import hashlib
import shutil
import sys
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
    success_message = ""
    try:
        data_path = os.path.abspath(data_path)

        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        file_hash = hash_file(data_path)
        if file_hash is None:
            return False, f"Failed to compute hash for {data_path}"

        with db.open_database(db_path) as conn:
            if title and db.dataset_exists(conn, None, title):
                return False, f"Dataset with name '{title}' already exists."
            exists_info = db.hash_exists(conn, file_hash)
            if exists_info:
                success_message = f"Warning: Version {exists_info} with same data already exists."

        try:
            _copy_file_to_objects(tracker_path, data_path, file_hash)
        except OSError as e:
            return False, f"Failed to copy file to objects directory: {e}"

        try:
            with db.open_database(db_path) as conn:
                dataset_id = db.insert_dataset(conn, title, notes)
                file_size = os.path.getsize(data_path)
                db.insert_object(conn, file_hash, file_size)
                db.insert_version(conn, dataset_id, file_hash, version, data_path)
                conn.commit()
                return_message = f"Data at {data_path} updated successfully"
                if success_message:
                    return_message = f"{success_message}\n\n{return_message}"
                return True, return_message
        except sqlite3.Error as e:
            removed, err = _remove_file_object(tracker_path, file_hash)
            if not removed:
                return False, f"Failed to remove object file {file_hash}: {err} | After database error: {e}"
            return False, f"Database error while adding data: {e}"
    except FileNotFoundError:
        return False, f"Data path {data_path} does not exist"
    except OSError as e:
        return False, f"File operation failed: {e}"

def _copy_file_to_objects(tracker_path: str, data_path: str, file_hash: str) -> None:
    """Copy a file to the objects directory"""
    save_path = os.path.join(tracker_path, "objects", file_hash)
    if os.path.isfile(data_path):
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.copy2(data_path, save_path)
    else:
        for filename in os.listdir(data_path): # figure out how to save, display and retrieve directories properly
            filepath = os.path.join(data_path, filename)
            _copy_file_to_objects(tracker_path, filepath, file_hash)
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
    success_message = ""
    try:
        data_path = os.path.abspath(data_path)

        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        file_hash = hash_file(data_path)
        if file_hash is None:
            return False, f"Failed to compute hash for {data_path}"

        with db.open_database(db_path) as conn:
            exists_info = db.hash_exists(conn, file_hash)
            if exists_info:
                success_message = f"Warning: {exists_info} with same data already exists."

        try:
            _copy_file_to_objects(tracker_path, data_path, file_hash)
        except OSError as e:
            return False, f"Failed to copy file to objects directory: {e}"

        with db.open_database(db_path) as conn:
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
        return_message = f"Data at {data_path} updated successfully"
        if success_message:
            return_message = f"{success_message}\n{return_message}"
        return True, return_message
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

def open_dataset_version(data_id: int, name: str, version_num: int) -> Tuple[bool, str]:
    """Open a dataset version by copying it to a temp file with proper extension
    """
    try:
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        all_versions = db.get_dataset_history(os.path.join(tracker_path, "tracker.db"), data_id, name)
        if not all_versions:
            return False, "No history found for the specified dataset."

        target_version = next((v for v in all_versions if v['version'] == version_num), None)
        if target_version is None:
            return False, f"Version {version_num} not found for the specified dataset."
        hash_name = target_version['object_hash']
        original_filepath = target_version['original_path']

        objects_path = os.path.join(tracker_path, "objects", hash_name)

        if not os.path.exists(objects_path):
            raise FileNotFoundError(f"Dataset version not found: {hash_name}")

        ext = os.path.splitext(original_filepath)[-1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as temp_file:
            temp_path = temp_file.name
            shutil.copy2(objects_path, temp_path)
        try:
            open_file(temp_path)
            return True, f"Opened dataset version {version_num} successfully."
        except OSError as e:
            os.unlink(temp_path)
            raise
    except sqlite3.Error as e:
        return False, f"Database error while opening dataset version: {e}"

def open_file(file_path: str) -> None:
    """Open a file using the default application based on the OS"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    try:
        if sys.platform == "win32": # Windows
            os.startfile(file_path)
        elif sys.platform == "darwin":  # macOS
            subprocess.run(["open", file_path], check=True)
        else:  # Linux and other Unix-like systems
            subprocess.run(["xdg-open", file_path], check=True)
    except Exception as e:
        raise OSError(f"Failed to open file: {e}")