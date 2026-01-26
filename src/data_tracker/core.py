import data_tracker.db_manager as db
from colorama import Fore, init # initialized in compare function only
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
        tracker_path = find_data_tracker_root()
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
        return_message = ""
        db_path = os.path.join(tracker_path, "tracker.db")

        with db.open_database(db_path) as conn:
            if dataset_id is None:
                if title and db.dataset_exists(conn, None, title):
                    return False, f"Dataset with name '{title}' already exists."
                dataset_id = db.insert_dataset(conn, title, message)
            else:
                if version is None:
                    version = db.get_next_version(conn, dataset_id)

            primary_hash = ""
            if len(files) == 1:
                primary_hash = hash_file(files[0][0])
            else:
                primary_hash = hash_directory(data_path)

            duplicate_warning = db.hash_exists(conn, primary_hash)
            if duplicate_warning:
                return_message = f"Duplicate Warning! Version with same data already exists: {duplicate_warning}\n"

            version_id = db.insert_version(conn, dataset_id, primary_hash, version, data_path, message)

            for filepath, rel_path in files:
                try:
                    file_hash = hash_file(filepath)
                    _copy_file_to_objects(tracker_path, filepath, file_hash)

                    file_size = os.path.getsize(filepath)
                    db.insert_object(conn, file_hash, file_size)
                    db.insert_files(conn, version_id, file_hash, rel_path)

                    return_message += f"Data at {filepath} added successfully" # change to updated for update
                except sqlite3.Error as exc:
                    file_hash = hash_file(filepath) # is this needed?
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

def _copy_file_to_objects(tracker_path: str, data_path: str, file_hash: str) -> None:
    """Copy a file to the objects directory"""
    save_path = os.path.join(tracker_path, "objects", file_hash)
    if os.path.isfile(data_path):
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.copy2(data_path, save_path)
    else:
        raise OSError("Directory handling not implemented yet") # remove this check?

def hash_file(file_path: str) -> str | None:
    """Compute the hash of a file for versioning using SHA256"""
    if not os.path.isfile(file_path):
        raise ValueError(f"{file_path} is not a valid file")
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def hash_directory(dir_path: str) -> str:
    """Compute hash of entire directory based on file contents and structure
     - Walk directory recursively but sort files and dirs for consistency
    """
    if not os.path.isdir(dir_path):
        raise ValueError(f"{dir_path} is not a valid directory")
    sha256_hash = hashlib.sha256()
    for root, dirs, files in os.walk(dir_path):
        dirs.sort()
        files.sort()

        for filename in files:
            filepath = os.path.join(root, filename)
            rel_path = os.path.relpath(filepath, dir_path)
            sha256_hash.update(rel_path.encode('utf-8'))
            with open(filepath, 'rb') as f:
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

def list_data(struct: bool) -> Tuple[bool, str]:
    """List all tracked data files in the data tracker.db datasets table
     - use the db_manager.py get_all_datasets function to retrieve datasets
     - format the output for display and return as a string
     - also display the folder structure for each dataset
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        all_datasets = db.get_all_datasets(os.path.join(tracker_path, "tracker.db"))
        if not all_datasets:
            return True, "No datasets tracked yet."

        all_datasets = sorted(all_datasets, key=lambda x: x['id'])
        output_lines = ["Tracked Datasets:"]
        for dataset in all_datasets:
            dataset_id = dataset['id']
            output_lines.append(f"ID: {dataset_id},  Name: {dataset['name']},  "
                                f"Created At: {dataset['created_at']},  Notes: {dataset['notes']}")
            if struct:
                structure = display_structure(db_path, dataset_id)
                output_lines.append(structure)
            output_lines.append("")

        return True, "\n".join(output_lines)
    except sqlite3.Error as e:
        return False, f"Database error while listing data: {e}"
    except OSError as e:
        return False, f"Filesystem error while listing data: {e}"

def display_structure(db_path: str, dataset_id: int, version: float = None) -> str:
    """Format the dataset folder structure for display and return as a string
     - Build a tree-like structure representing files and folders with helper function
     - If version is None, use the latest version
    """
    try:
        all_versions = db.get_dataset_history(db_path, dataset_id, None)
        if not all_versions:
            return "  No versions found for this dataset."

        latest_version = all_versions[-1]['version']
        original_path = all_versions[-1]['original_path']

        all_files = db.get_files_for_version(db_path, dataset_id, None, version if version else latest_version)

        if len(all_files) == 1 and all_files[0]['relative_path'] == os.path.basename(original_path):
            root_name = os.path.basename(original_path)
            return f"  Structure:\n    -{root_name}"
        else:
            root_name = os.path.basename(original_path.rstrip(os.sep))
            lines = [f"  Structure:", f"    {root_name}/"]

            sorted_files = sorted(all_files, key=lambda x: x['relative_path'])

            tree_dict = {}
            for file_record in sorted_files:
                rel_path = file_record['relative_path']
                parts = rel_path.split(os.sep)

                current_level = tree_dict
                for part in parts[:-1]:
                    if part not in current_level:
                        current_level[part] = {}
                    current_level = current_level[part]

                file_name = parts[-1]
                current_level[file_name] = None

            def format_tree(tree, prefix="      "):
                """Format the tree structure recursively and return as a list of strings"""
                items = sorted(tree.items(), key=lambda x: (x[1] is not None, x[0]))
                result = []

                for idx, (name, subtree) in enumerate(items):
                    is_last_item = (idx == len(items) - 1)
                    connector = "└── " if is_last_item else "├── "

                    if subtree is None:  # File
                        result.append(f"{prefix}{connector}{name}")
                    else:  # Directory
                        result.append(f"{prefix}{connector}{name}/")
                        extension = "    " if is_last_item else "│   "
                        result.extend(format_tree(subtree, prefix + extension))
                return result

            lines.extend(format_tree(tree_dict))
            return "\n".join(lines)
    except (sqlite3.Error, OSError, KeyError, IndexError) as e:
        return f"Failed to retrieve structure: {e}"

def get_history(data_id: int, name: str, detailed_flag: bool) -> Tuple[bool, str]:
    """Show history of versions with additional info for a specific data id or name
     - format the output for display based on detailed_flag and return as str
     - if detailed_flag is True, show Version, ID, Message, Added At, Original Path, Object Hash
     - else show Version, Message, Added At
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        entire_history = db.get_dataset_history(os.path.join(tracker_path, "tracker.db"), data_id, name)
        if not entire_history:
            return True, "No history found for the specified dataset."

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
    """Add a new version of existing dataset to the tracker and tracker.db"""
    try:
        tracker_path = find_data_tracker_root()
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
        tracker_path = find_data_tracker_root()
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
    object_path = os.path.join(tracker_path, "objects", file_hash) # add a check if the object is not being used
    try:
        os.remove(object_path)
    except FileNotFoundError:
        pass
    except OSError as e:
        return False, f"Failed to remove object file {object_path}: {e}"
    return True, ""

def open_dataset_version(data_id: int, name: str, version_num: float) -> Tuple[bool, str]:
    """Open a dataset version by copying it to a temp file with proper extension
     - At the start run cleanup_temp_files to remove old temp files from filesystem
     - If single file, create a named temp file with the original extension and open it
     - If multiple files, create a temp directory, reconstruct folder structure, and open it
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        removed, failed = cleanup_temp_files()
        if failed > 0:
            print(f"Warning: Failed to remove {failed} temporary file(s) from previous view commands.")

        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        all_files = db.get_files_for_version(os.path.join(tracker_path, "tracker.db"), data_id, name, version_num)
        if not all_files:
            return False, "No files found for the specified dataset version."

        objects_path = os.path.join(tracker_path, "objects")

        if len(all_files) == 1:
            file_record = all_files[0]
            file_hash = file_record['object_hash']
            rel_path = file_record['relative_path']
            source = os.path.join(objects_path, file_hash)

            if not os.path.exists(source):
                raise FileNotFoundError(f"Dataset version not found: {file_hash}")

            ext = os.path.splitext(rel_path)[-1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix="dt_view_") as temp_file:
                temp_path = temp_file.name
                shutil.copy2(source, temp_path)
            try:
                open_file(temp_path)
            except OSError as e:
                os.unlink(temp_path)
                raise
        else:
            temp_dir = tempfile.mkdtemp(prefix="dt_view_")
            try:
                for file_record in all_files:
                    file_hash = file_record['object_hash']
                    rel_path = file_record['relative_path']
                    source = os.path.join(objects_path, file_hash)
                    dest = os.path.join(temp_dir, rel_path)

                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    shutil.copy2(source, dest)
                open_file(temp_dir)
            except OSError as e:
                shutil.rmtree(temp_dir, ignore_errors=True)
                raise

        return True, f"Opened dataset {data_id}, version {version_num} successfully."
    except sqlite3.Error as e:
        return False, f"Database error while opening dataset {data_id} version {version_num}: {e}"
    except OSError as e:
        return False, f"Filesystem error while opening dataset {data_id} version {version_num}: {e}"

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

def cleanup_temp_files() -> Tuple[int, int]:
    """Remove temporary files created by previous view commands"""
    removed: int = 0
    failed: int = 0
    try:
        temp_dir = tempfile.gettempdir()
        dt_pattern = "dt_view_"

        for item in os.listdir(temp_dir):
            if item.startswith(dt_pattern):
                item_path = os.path.join(temp_dir, item)
                try:
                    if os.path.isfile(item_path):
                        os.unlink(item_path)
                        removed += 1
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        removed += 1
                except OSError:
                    failed += 1
    except OSError:
        pass
    return removed, failed

def compare_dataset_versions(data_id: int, name: str, version_1: float, version_2: float) -> Tuple[bool, str]:
    """Compare two versions of a dataset and show differences
     - List added, removed, and modified files between versions
     - Show size differences of the files and total size changes
     - Format output with color coding for clarity
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        init() # initialize colorama
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        files_v1 = db.get_files_for_version(db_path, data_id, name, version_1)
        files_v2 = db.get_files_for_version(db_path, data_id, name, version_2)
        if not files_v1:
            return False, f"No files found for version {version_1}. Version may be invalid."
        if not files_v2:
            return False, f"No files found for version {version_2}. Version may be invalid."
        set_v1: set = {(file['relative_path'], file['object_hash'],
                        db.get_object_size(db_path, file['object_hash'])) for file in files_v1}

        set_v2: set = {(file['relative_path'], file['object_hash'],
                        db.get_object_size(db_path, file['object_hash'])) for file in files_v2}

        if set_v1 == set_v2:
            return True, f"No differences between version {version_1} and version {version_2}"

        output_lines = [f"Comparison between version {version_1} and version {version_2}:", f"Version: {version_1}"]

        structure_1 = display_structure(db_path, data_id, version_1)
        output_lines.append(structure_1)
        structure_2 = display_structure(db_path, data_id, version_2)
        output_lines.append(f"\nVersion: {version_2}")
        output_lines.append(structure_2)

        added_files = set_v2 - set_v1
        removed_files = set_v1 - set_v2
        total_size_added = sum(size for _, _, size in added_files)
        total_size_removed = sum(size for _, _, size in removed_files)

        modified_files: set = set()
        for path_v1, hash_v1, size_v1 in set_v1:
            for path_v2, hash_v2, size_v2 in set_v2:
                if path_v1 == path_v2 and hash_v1 != hash_v2:
                    modified_files.add((path_v1, size_v1, size_v2))
                    break

        if modified_files:
            output_lines.append("\nModified files:")
            total_size_diff = 0
            for rel_path, old_size, new_size in sorted(modified_files):
                size_change = new_size - old_size
                sign = "+" if size_change > 0 else ""
                total_size_diff += size_change
                output_lines.append(f"  {Fore.YELLOW}~ {rel_path} | Size: {format_size(old_size)} →"
                                    f"{format_size(new_size)} = {sign}{format_size(size_change)}{Fore.RESET}")
            output_lines.append(f"Total size change: {format_size(total_size_diff)}")

        if added_files:
            output_lines.append("\nAdded files:")
            for rel_path, obj_hash, size in added_files:
                output_lines.append(f"  {Fore.GREEN}+ {rel_path} | Size: {format_size(size)}{Fore.RESET}")
            output_lines.append(f"Total size added: {format_size(total_size_added)}")
        if removed_files:
            output_lines.append("Removed files:")
            for rel_path, obj_hash, size in removed_files:
                output_lines.append(f"  {Fore.RED}- {rel_path} | Size: {format_size(size)}{Fore.RESET}")
            output_lines.append(f"Total size removed: {format_size(total_size_removed)}")
        if not added_files:
            output_lines.append("No files added.")
        if not removed_files:
            output_lines.append("No files removed.")
        if not modified_files:
            output_lines.append("No files modified.")

        return True, "\n".join(output_lines)
    except sqlite3.Error as e:
        return False, f"Database error while comparing dataset versions: {e}"
    except OSError as e:
        return False, f"Filesystem error while comparing dataset versions: {e}"

def format_size(size_in_bytes: int) -> str:
    """Format size in bytes to a human-readable string"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} PB"