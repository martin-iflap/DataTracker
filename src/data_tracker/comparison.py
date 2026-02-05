import data_tracker.file_utils as fu
import data_tracker.db_manager as db
from colorama import Fore, init # init colorama in compare_dataset_versions
from typing import Tuple
import sqlite3
import difflib
import os


def compare_dataset_versions(dataset_id: int, name: str,
                             version_1: float, version_2: float) -> Tuple[bool, str]:
    """Compare two versions of a dataset and show differences
     - List added, removed, and modified files between versions
     - Show size differences of the files and total size changes
     - Format output with color coding for clarity
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        init() # initialize colorama
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        if version_1 is None:
            with db.open_database(db_path) as conn:
                version_1 = db.get_first_version(conn, dataset_id)
                if version_1 is None:
                    return False, f"Could not determine first version for dataset {name if name else ""} ID:{dataset_id}."

        if version_2 is None:
            with db.open_database(db_path) as conn:
                version_2 = db.get_latest_version(conn, dataset_id)
                if version_2 == 0.0:
                    return False, f"Could not determine latest version for dataset {name if name else ""} ID:{dataset_id}."

        if version_1 == version_2:
            return False, "Cannot compare the same version to itself (file might have only 1 version)."

        files_v1 = db.get_files_for_version(db_path, dataset_id, name, version_1)
        files_v2 = db.get_files_for_version(db_path, dataset_id, name, version_2)
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

        structure_1 = fu.display_structure(db_path, dataset_id, version_1)
        output_lines.append(structure_1)
        structure_2 = fu.display_structure(db_path, dataset_id, version_2)
        output_lines.append(f"\nVersion: {version_2}")
        output_lines.append(structure_2)

        modified_files: set = set()
        for path_v1, hash_v1, size_v1 in set_v1:
            for path_v2, hash_v2, size_v2 in set_v2:
                if path_v1 == path_v2 and hash_v1 != hash_v2:
                    modified_files.add((path_v1, hash_v1, hash_v2, size_v1, size_v2))
                    break

        modified_paths = {path for path, _, _, _, _ in modified_files}
        added_files = {(path, f_hash, size) for path, f_hash, size in (set_v2 - set_v1) if path not in modified_paths}
        removed_files = {(path, f_hash, size) for path, f_hash, size in (set_v1 - set_v2) if path not in modified_paths}
        total_size_added = sum(size for _, _, size in added_files)
        total_size_removed = sum(size for _, _, size in removed_files)

        if modified_files:
            output_lines.append("\nModified files:")
            total_size_diff = 0
            for rel_path, hash_v1, hash_v2, old_size, new_size in sorted(modified_files):
                size_change = new_size - old_size
                sign = "+" if size_change > 0 else ""
                total_size_diff += size_change
                output_lines.append(f"  {Fore.YELLOW}~ {rel_path} | Size: {fu.format_size(old_size)} → "
                                    f"{fu.format_size(new_size)} = {sign}{fu.format_size(size_change)}{Fore.RESET}")

                similarity, added, removed = compare_files(tracker_path, hash_v1, hash_v2)
                if not similarity:
                    output_lines.append("    Could not compare files (files might be missing or corrupted)")
                else:
                    output_lines.append(f"    Similarity: {similarity:.2f}%")
                if added is not None and removed is not None:
                    output_lines.append(f"    Lines added: {Fore.GREEN}{added}{Fore.RESET}, Lines removed: {Fore.RED}{removed}{Fore.RESET}")

            output_lines.append(f"Total size change: {Fore.YELLOW}{fu.format_size(total_size_diff)}{Fore.RESET}")

        if added_files:
            output_lines.append("\nAdded files:")
            for rel_path, obj_hash, size in added_files:
                output_lines.append(f"  {Fore.GREEN}+ {rel_path} | Size: {fu.format_size(size)}{Fore.RESET}")
            output_lines.append(f"Total size added: {Fore.GREEN}{fu.format_size(total_size_added)}{Fore.RESET}")
        if removed_files:
            output_lines.append("\nRemoved files:")
            for rel_path, obj_hash, size in removed_files:
                output_lines.append(f"  {Fore.RED}- {rel_path} | Size: {fu.format_size(size)}{Fore.RESET}")
            output_lines.append(f"Total size removed: {Fore.RED}{fu.format_size(total_size_removed)}{Fore.RESET}")
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

def compare_files(tracker_path: str, hash_v1: str, hash_v2: str) -> Tuple[float, int, int]:
    """Compare two files and return similarity percentage and line changes
     - Handles text files line-by-line and binary files byte-by-byte by using helper function
    Returns:
        Tuple[float, int, int, int]: (similarity_percentage, lines_added, lines_removed)
    """
    objects_path = os.path.join(tracker_path, "objects")
    file1 = os.path.join(objects_path, hash_v1)
    file2 = os.path.join(objects_path, hash_v2)
    if not os.path.exists(file1) or not os.path.exists(file2):
        raise FileNotFoundError("One or both files do not exist")

    if _is_binary(file1) or _is_binary(file2):
        return _compare_binary_files(file1, file2)

    with open(file1, 'r', encoding='utf-8', errors='ignore') as f1:
        lines1 = f1.readlines()
    with open(file2, 'r', encoding='utf-8', errors='ignore') as f2:
        lines2 = f2.readlines()

    matcher = difflib.SequenceMatcher(None, lines1, lines2)
    similarity = matcher.ratio() * 100

    diff = list(difflib.unified_diff(lines1, lines2, lineterm=''))
    added = sum(1 for line in diff if line.startswith('+') and not line.startswith('+++'))
    removed = sum(1 for line in diff if line.startswith('-') and not line.startswith('---'))
    return similarity, added, removed

def _is_binary(file_path: str) -> bool:
    """Check if a file is binary by reading its first 8192 bytes
     - check for null bytes and non-text characters ratio
    """
    chunk_size = 8192
    with open(file_path, 'rb') as f:
        chunk = f.read(chunk_size)
    if not chunk:
        return False

    if chunk.count(b'\0') > 1:
        return True

    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
    non_text = chunk.translate(None, text_chars)
    return len(non_text) / len(chunk) > 0.30

def _compare_binary_files(file1: str, file2: str) -> Tuple[float, None, None]:
    """Compare binary files byte-by-byte
     - called by the compare_files function when text read fails
    Returns: Tuple[float, int, int, int]: (similarity_percentage, None, None)
    """
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        bytes1 = f1.read()
        bytes2 = f2.read()
    matcher = difflib.SequenceMatcher(None, bytes1, bytes2)
    similarity = matcher.ratio() * 100
    return similarity, None, None