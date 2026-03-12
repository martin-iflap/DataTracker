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
     - If both versions are None, auto-compare the two most recent versions
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        init() # initialize colorama
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        if dataset_id is None:
            with db.open_database(db_path) as conn:
                dataset_id = db.get_id_from_name(conn, name)
                if dataset_id is None:
                    return False, f"Dataset with name '{name}' not found."

        if version_1 is None:
            with db.open_database(db_path) as conn:
                version_1 = db.get_second_latest_version(conn, dataset_id)
                if version_1 is None:
                    return False, f"Dataset {name if name else ''} ID:{dataset_id} needs at least 2 versions to auto-compare."

        if version_2 is None:
            with db.open_database(db_path) as conn:
                version_2 = db.get_latest_version(conn, dataset_id)
                if version_2 == 0.0:
                    return False, f"Could not determine latest version for dataset {name if name else ""} ID:{dataset_id}."

        if version_1 == version_2:
            return False, "Cannot compare a version to itself. Provide two different version numbers."

        files_v1 = db.get_files_for_version(db_path, dataset_id, name, version_1)
        files_v2 = db.get_files_for_version(db_path, dataset_id, name, version_2)
        if not files_v1:
            return False, f"No files found for version {version_1}. Version may be invalid."
        if not files_v2:
            return False, f"No files found for version {version_2}. Version may be invalid."

        objects_path = os.path.join(tracker_path, "objects")
        set_v1: set = {(f['relative_path'], f['object_hash'],
                        db.get_object_size(db_path, f['object_hash'])) for f in files_v1}
        set_v2: set = {(f['relative_path'], f['object_hash'],
                        db.get_object_size(db_path, f['object_hash'])) for f in files_v2}

        if set_v1 == set_v2:
            return True, f"No differences between version {version_1} and version {version_2}"

        header = [
            f"Comparison between version {version_1} and version {version_2}:",
            f"Version: {version_1}",
            fu.display_structure(db_path, dataset_id, version_1),
            f"\nVersion: {version_2}",
            fu.display_structure(db_path, dataset_id, version_2),
        ]

        def path_for_hash(h):
            return os.path.join(objects_path, h)

        return True, _format_comparison_output(
            set_v1, set_v2, header, path_for_hash, path_for_hash
        )
    except sqlite3.Error as e:
        return False, f"Database error while comparing dataset versions: {e}"
    except OSError as e:
        return False, f"Filesystem error while comparing dataset versions: {e}"


def diff_dataset(dataset_id: int, name: str, version: float | None) -> Tuple[bool, str]:
    """Show differences between a stored version and the current live state of the dataset.
     - Compares the files recorded in the given version against the files on disk
       at the path recorded in that version's original_path.
     - If version is None, uses the latest version.
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        init()
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        if dataset_id is None:
            with db.open_database(db_path) as conn:
                dataset_id = db.get_id_from_name(conn, name)

        if version is None:
            with db.open_database(db_path) as conn:
                version = db.get_latest_version(conn, dataset_id)
                if version == 0.0:
                    return False, "No versions found for the specified dataset."

        stored_files = db.get_files_for_version(db_path, dataset_id, name, version)
        if not stored_files:
            return False, f"No files found for version {version}. Version may be invalid."

        latest_info = db.get_latest_version_info(db_path, dataset_id)
        live_root = latest_info['original_path']

        if not os.path.exists(live_root):
            return False, (
                f"Live path not found: {live_root}\n"
                f"The dataset may have been moved since it was last tracked.\n"
                f"Use 'dt status' to review tracked paths."
            )

        objects_path = os.path.join(tracker_path, "objects")

        # Build stored set from objects store
        set_stored: set = {
            (f['relative_path'], f['object_hash'],
             db.get_object_size(db_path, f['object_hash']))
            for f in stored_files
        }

        # Build live set and hash -> fpath lookup
        set_live: set = set()
        live_lookup: dict = {}
        if os.path.isfile(live_root):
            file_hash = fu.hash_file(live_root)
            file_size = os.path.getsize(live_root)
            rel_path = os.path.basename(live_root)
            set_live.add((rel_path, file_hash, file_size))
            live_lookup[file_hash] = live_root
        else:
            for root, _, filenames in os.walk(live_root):
                for f_name in filenames:
                    fpath = os.path.join(root, f_name)
                    rel = os.path.relpath(fpath, live_root)
                    file_hash = fu.hash_file(fpath)
                    file_size = os.path.getsize(fpath)
                    set_live.add((rel, file_hash, file_size))
                    live_lookup[file_hash] = fpath

        if not set_live:
            return False, f"No files found at live path: {live_root}"

        if set_stored == set_live:
            return True, f"No differences — live files match version {version}."

        header = [
            f"Diff: version {version} vs live",
            f"Version {version} (stored):",
            fu.display_structure(db_path, dataset_id, version),
            f"\nLive ({live_root}):",
        ]

        def stored_path(h):
            return os.path.join(objects_path, h)

        def live_path(h):
            return live_lookup.get(h)

        return True, _format_comparison_output(
            set_stored, set_live, header, stored_path, live_path
        )
    except ValueError as e:
        return False, str(e)
    except sqlite3.Error as e:
        return False, f"Database error during diff: {e}"
    except OSError as e:
        return False, f"Filesystem error during diff: {e}"


def _format_comparison_output(
    set_v1: set,
    set_v2: set,
    header_lines: list,
    resolve_path_v1,
    resolve_path_v2,
) -> str:
    """Format the diff output between two file sets.
     - set_v1 / set_v2: sets of (relative_path, object_hash, size) tuples
     - header_lines: list of strings printed before the diff body
     - resolve_path_v1 / resolve_path_v2: callables that take a hash and return
       an absolute file path for that side, used by compare_files
    Returns: str — the full formatted output
    """
    modified_files: set = set()
    for path_v1, hash_v1, size_v1 in set_v1:
        for path_v2, hash_v2, size_v2 in set_v2:
            if path_v1 == path_v2 and hash_v1 != hash_v2:
                modified_files.add((path_v1, hash_v1, hash_v2, size_v1, size_v2))
                break

    modified_paths = {path for path, _, _, _, _ in modified_files}
    added_files   = {(p, h, s) for p, h, s in (set_v2 - set_v1) if p not in modified_paths}
    removed_files = {(p, h, s) for p, h, s in (set_v1 - set_v2) if p not in modified_paths}
    total_size_added   = sum(s for _, _, s in added_files)
    total_size_removed = sum(s for _, _, s in removed_files)

    output_lines = list(header_lines)

    if modified_files:
        output_lines.append("\nModified files:")
        total_size_diff = 0
        for rel_path, hash_v1, hash_v2, old_size, new_size in sorted(modified_files):
            size_change = new_size - old_size
            sign = "+" if size_change > 0 else ""
            total_size_diff += size_change
            output_lines.append(
                f"  {Fore.YELLOW}~ {rel_path} | Size: {fu.format_size(old_size)} → "
                f"{fu.format_size(new_size)} = {sign}{fu.format_size(size_change)}{Fore.RESET}"
            )
            try:
                p1 = resolve_path_v1(hash_v1)
                p2 = resolve_path_v2(hash_v2)
                if p1 and p2:
                    similarity, added, removed = compare_files(p1, p2)
                    if not similarity:
                        output_lines.append("    Could not compare files (files might be missing or corrupted)")
                    else:
                        output_lines.append(f"    Similarity: {similarity:.2f}%")
                    if added is not None and removed is not None:
                        output_lines.append(
                            f"    Lines added: {Fore.GREEN}{added}{Fore.RESET}, "
                            f"Lines removed: {Fore.RED}{removed}{Fore.RESET}"
                        )
                else:
                    output_lines.append("    Could not locate file for detailed comparison.")
            except OSError:
                output_lines.append("    Could not compare files (I/O error).")
        output_lines.append(f"Total size change: {Fore.YELLOW}{fu.format_size(total_size_diff)}{Fore.RESET}")

    if added_files:
        output_lines.append("\nAdded files:")
        for rel_path, _, size in sorted(added_files):
            output_lines.append(f"  {Fore.GREEN}+ {rel_path} | Size: {fu.format_size(size)}{Fore.RESET}")
        output_lines.append(f"Total size added: {Fore.GREEN}{fu.format_size(total_size_added)}{Fore.RESET}")

    if removed_files:
        output_lines.append("\nRemoved files:")
        for rel_path, _, size in sorted(removed_files):
            output_lines.append(f"  {Fore.RED}- {rel_path} | Size: {fu.format_size(size)}{Fore.RESET}")
        output_lines.append(f"Total size removed: {Fore.RED}{fu.format_size(total_size_removed)}{Fore.RESET}")

    if not added_files:
        output_lines.append("No files added.")
    if not removed_files:
        output_lines.append("No files removed.")
    if not modified_files:
        output_lines.append("No files modified.")

    return "\n".join(output_lines)


def compare_files(file_path_1: str, file_path_2: str) -> Tuple[float, int, int]:
    """Compare two files by their absolute paths and return similarity and line changes.
     - Handles text files line-by-line and binary files byte-by-byte.
    Returns: Tuple[float, int | None, int | None]
    """
    if not os.path.exists(file_path_1) or not os.path.exists(file_path_2):
        raise FileNotFoundError("One or both files do not exist")

    if _is_binary(file_path_1) or _is_binary(file_path_2):
        return _compare_binary_files(file_path_1, file_path_2)

    with open(file_path_1, 'r', encoding='utf-8', errors='ignore') as f1:
        lines1 = f1.readlines()
    with open(file_path_2, 'r', encoding='utf-8', errors='ignore') as f2:
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
