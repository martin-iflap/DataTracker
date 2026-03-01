import data_tracker.db_manager as db
from colorama import Fore, init # init colorama in get_stats
from typing import Tuple
import subprocess
import tempfile
import sqlite3
import hashlib
import shutil
import sys
import os


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

def copy_file_to_objects(tracker_path: str, data_path: str, file_hash: str) -> None:
    """Copy a file to the objects directory"""
    save_path = os.path.join(tracker_path, "objects", file_hash)
    if os.path.isfile(data_path):
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.copy2(data_path, save_path)
    else:
        raise OSError("Directory passed to copy_file_to_objects, expected a file: " + data_path)

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
        target_version = version if version else latest_version
        version_record = next(
            (v for v in all_versions if v['version'] == target_version), all_versions[-1]
        )
        original_path = version_record['original_path']

        all_files = db.get_files_for_version(db_path, dataset_id, None, target_version)

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

def format_size(size_in_bytes: int) -> str:
    """Format size in bytes to a human-readable string"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} PB"

def export_file(export_path: str, data_id: int, name: str,
                version: float, force: bool, preserve_root: bool) -> Tuple[bool, str]:
    """Export a specific dataset version to a given path
     - If single file, copy to export_path (create parent dirs if needed)
     - If multiple files, create export_path as directory and reconstruct structure
    """
    try:
        if not export_path or not export_path.strip():
            return False, "Export path cannot be empty."

        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        all_files = db.get_files_for_version(db_path, data_id, name, version)
        if not all_files:
            return False, f"No files found for dataset version {version}. Version may not exist."

        objects_path = os.path.join(tracker_path, "objects")

        if len(all_files) == 1:
            file_record = all_files[0]
            file_hash = file_record['object_hash']
            rel_path = file_record['relative_path']
            source = os.path.join(objects_path, file_hash)

            if not os.path.exists(source):
                raise FileNotFoundError(f"Object file not found: {file_hash}")

            if os.path.isdir(export_path):
                dest = os.path.join(export_path, os.path.basename(rel_path))
            else:
                dest = export_path
                parent_dir = os.path.dirname(dest)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)

            if os.path.exists(dest) and not force:
                return False, f"Export path already exists: {dest}. Use --force to overwrite."
            shutil.copy2(source, dest)
        else:
            if preserve_root:
                dataset_history = db.get_dataset_history(db_path , data_id, name)
                if not dataset_history:
                    return False, f"Failed to retrieve root directory name for dataset ID {data_id}."
                original_path = dataset_history[0]['original_path']
                root_dir_name = os.path.basename(original_path.rstrip(os.sep))
                export_path = os.path.join(export_path, root_dir_name)

            if os.path.exists(export_path) and not os.path.isdir(export_path):
                return False, f"Export path '{export_path}' exists but is not a directory."
            os.makedirs(export_path, exist_ok=True)

            for file_record in all_files:
                file_hash = file_record['object_hash']
                rel_path = file_record['relative_path']
                source = os.path.join(objects_path, file_hash)

                if not os.path.exists(source):
                    raise FileNotFoundError(f"Object file not found: {file_hash}")

                dest = os.path.join(export_path, rel_path)
                if os.path.exists(dest) and not force:
                    return False, f"File already exists: {dest}. Use --force to overwrite."

                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.copy2(source, dest)
        return True, f"Exported version {version} to {export_path} successfully."
    except FileNotFoundError as e:
        return False, str(e)
    except sqlite3.Error as e:
        return False, f"Database error while exporting: {e}"
    except OSError as e:
        return False, f"Filesystem error while exporting: {e}"

def get_storage_stats() -> Tuple[bool, str]:
    """Get the number of files and the total size of all the objects"""
    try:
        tracker_path = find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."

        objects_path = os.path.join(tracker_path, "objects")
        total_files = 0
        total_size = 0

        init()

        for root, dirs, files in os.walk(objects_path):
            total_files += len(files)
            for file in files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)

        return True, (f"Total number of files in data_tracker/objects directory: {Fore.YELLOW}{total_files}{Fore.RESET}, "
                      f"Total size: {Fore.YELLOW}{format_size(total_size)}{Fore.RESET}")
    except OSError as e:
        return False, f"Filesystem error while calculating stats: {e}"