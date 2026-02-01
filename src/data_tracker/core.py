import data_tracker.db_manager as db
import data_tracker.file_utils as fu
from typing import Tuple
import sqlite3
import os


def initialize_tracker() -> Tuple[bool, str]:
    """Initialize the .data_tracker directory and config.json file
    Returns: Tuple[bool, str]: (success, message)
    """
    existing_tracker = fu.find_data_tracker_root()
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

def add_data(data_path: str, title: str, version: float, message: str) -> Tuple[bool, str]:
    """Add new data file or folder to be tracked
     - If file: adds single file with its hash
     - If folder: recursively adds all files preserving folder structure
     - Computes hash for each file and stores in objects directory
     - Records dataset, object, version, and file structure info in database
     - Generates default name (dataset-<num>) if no title provided
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        data_path = os.path.abspath(data_path)
        if not os.path.exists(data_path):
            return False, f"Data path {data_path} does not exist"

        if os.path.isfile(data_path):
            files_to_add = [(data_path, os.path.basename(data_path))]
        else:
            files_to_add = []
            for root, _, filenames in os.walk(data_path):
                for f_name in filenames:
                    filepath = os.path.join(root, f_name)
                    rel_path = os.path.relpath(filepath, data_path)
                    files_to_add.append((filepath, rel_path))

        return _add_files_to_tracker(files_to_add, tracker_path, data_path,
                                     title=title, version=version, message=message)
    except FileNotFoundError:
        return False, f"Data path {data_path} does not exist"
    except OSError as e:
        return False, f"File operation failed: {e}"
    except ValueError as e:
        return False, str(e)

def _add_files_to_tracker(files: list[Tuple[str, str]], tracker_path: str,
                          data_path: str, dataset_id: int = None, title: str = None,
                          version: float = None, message: str = None) -> Tuple[bool, str]:
    """Add multiple files to the data tracker in a single transaction
     - Creates dataset entry and inserts all files atomically
     - Each file gets hashed, copied to objects, and recorded in files table
     - `relative_path` preserves folder structure for later reconstruction
     - Warns about hash collisions but doesn't prevent insertion
    Returns: Tuple[bool, str]: (success, concatenated messages for all files)"""
    try:
        return_message: str = ""
        db_path: str = os.path.join(tracker_path, "tracker.db")
        action: str = "added" if dataset_id is None else "updated"

        with db.open_database(db_path) as conn:
            if version and not db.check_version_exists(conn, dataset_id, version):
                return False, f"Version {version} already exists for the specified dataset."

            if dataset_id is None:
                if title and db.dataset_exists(conn, None, title):
                    return False, f"Dataset with name '{title}' already exists."
                dataset_id = db.insert_dataset(conn, title, message)
            else:
                if version is None:
                    version = db.get_latest_version(conn, dataset_id) + 1

            primary_hash: str = ""
            if len(files) == 1:
                primary_hash = fu.hash_file(files[0][0])
            else:
                primary_hash = fu.hash_directory(data_path)

            duplicate_warning = db.hash_exists(conn, primary_hash)
            if duplicate_warning:
                return_message = f"Duplicate Warning! Version with same data already exists: {duplicate_warning}\n"

            version_id = db.insert_version(conn, dataset_id, primary_hash, version, data_path, message)

            for filepath, rel_path in files:
                try:
                    file_hash = fu.hash_file(filepath)
                    fu.copy_file_to_objects(tracker_path, filepath, file_hash)

                    file_size = os.path.getsize(filepath)
                    db.insert_object(conn, file_hash, file_size)
                    db.insert_files(conn, version_id, file_hash, rel_path)

                    return_message += f"Data at {filepath} {action} successfully"
                except sqlite3.Error as exc:
                    if not db.object_is_used(conn, file_hash):
                        removed, err = _remove_file_object(tracker_path, file_hash)
                        if not removed:
                            return False, f"Failed to remove object file {file_hash}: {err} | After database error: {exc}"
                    return False, f"Database error while adding data: {exc}"
            conn.commit()
    except sqlite3.Error as e:
        return False, f"Database error while adding data: {e}"
    except OSError as e:
        return False, f"Filesystem error while adding data: {e}"
    except Exception as e:
        return False, f"Error while adding data: {e}"
    return True, return_message.strip()

def list_data(struct: bool) -> Tuple[bool, str]:
    """List all tracked data files in the data tracker.db datasets table
     - use the db_manager.py get_all_datasets function to retrieve datasets
     - format the output for display and return as a string
     - also display the folder structure for each dataset
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        all_datasets = db.get_all_datasets(db_path)
        if not all_datasets:
            return True, "No datasets tracked yet."

        all_datasets = sorted(all_datasets, key=lambda x: x['id'])
        output_lines = ["Tracked Datasets:"]
        for dataset in all_datasets:
            dataset_id = dataset['id']
            output_lines.append(f"ID: {dataset_id},  Name: {dataset['name']},  "
                                f"Created At: {dataset['created_at']},  Notes: {dataset['notes']}")
            if struct:
                structure = fu.display_structure(db_path, dataset_id)
                output_lines.append(structure)
            output_lines.append("")

        return True, "\n".join(output_lines)
    except sqlite3.Error as e:
        return False, f"Database error while listing data: {e}"
    except OSError as e:
        return False, f"Filesystem error while listing data: {e}"

def get_history(data_id: int, name: str, detailed_flag: bool) -> Tuple[bool, str]:
    """Show history of versions with additional info for a specific data id or name
     - format the output for display based on detailed_flag and return as str
     - if detailed_flag is True, show Version, ID, Message, Added At, Original Path, Object Hash
     - else show Version, Message, Added At
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        entire_history = db.get_dataset_history(os.path.join(tracker_path, "tracker.db"), data_id, name)
        if not entire_history:
            return True, "No history found for the specified dataset. ID may be invalid."

        output_lines = ["Dataset History:"]
        for record in entire_history:
            if detailed_flag:
                output_lines.append(
                    f"Version: {float(record['version'])},  ID: {record['id']},  Message: {record['message']}, "
                    f"Added At: {record['created_at']},  Original Path: {record['original_path']},  Object Hash: {record['object_hash']}\n"
                )
            else:
                output_lines.append(
                    f"Version: {float(record['version'])},  Message: {record['message']},  Added At: {record['created_at']}"
                )
        return True, "\n".join(output_lines)
    except sqlite3.Error as e:
        return False, f"Database error while retrieving history: {e}"
    except OSError as e:
        return False, f"Filesystem error while retrieving history: {e}"

def update_data(data_path: str, data_id: int, name: str, version: float, message: str) -> Tuple[bool, str]:
    """Add a new version of existing dataset to the tracker and tracker.db
     - Similar to add_data but requires existing dataset id or name
     - Validates dataset existence before adding new version
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        data_path = os.path.abspath(data_path)
        if not os.path.exists(data_path):
            return False, f"Data path {data_path} does not exist"
        db_path = os.path.join(tracker_path, "tracker.db")

        if os.path.isfile(data_path):
            files_to_add = [(data_path, os.path.basename(data_path))]
        else:
            files_to_add = []
            for root, _, filenames in os.walk(data_path):
                for f_name in filenames:
                    filepath = os.path.join(root, f_name)
                    rel_path = os.path.relpath(filepath, data_path)
                    files_to_add.append((filepath, rel_path))

        with db.open_database(db_path) as conn:
            if not db.dataset_exists(conn, data_id, name):
                return False, "Dataset does not exist. Run 'dt add' first."
            if not data_id:
                data_id = db.get_id_from_name(conn, name)

        return _add_files_to_tracker(files_to_add, tracker_path, data_path,
                                     dataset_id=data_id, version=version, message=message)
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
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        with db.open_database(os.path.join(tracker_path, "tracker.db")) as conn:
            exists = db.dataset_exists(conn, data_id, name)
            if not exists:
                return False, "Specified dataset does not exist."

            if data_id is None:
                data_id = db.get_id_from_name(conn, name)

            db.delete_files(conn, data_id)
            db.delete_versions(conn, data_id)
            hashes_to_remove = db.delete_object(conn)
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