import data_tracker.file_utils as fu
import data_tracker.db_manager as db
from colorama import Fore, init
from typing import Tuple
import sqlite3
import os


def get_status(detailed: bool) -> Tuple[bool, str]:
    """Get the status of all tracked datasets.
     - Compares the live filesystem hash at each dataset's latest version's recorded
       original_path against the stored version hashes.
     - Default: shows status against the latest version only.
     - Detailed: also reports which older version matches, if any.
    Returns: Tuple[bool, str]: (success, message)
    """
    try:
        init() # initialize colorama
        tracker_path = fu.find_data_tracker_root()
        if tracker_path is None:
            return False, "Data tracker is not initialized. Please run 'dt init' first."
        db_path = os.path.join(tracker_path, "tracker.db")

        all_datasets = db.get_all_datasets(db_path)
        if not all_datasets:
            return True, "Data tracker is initialized but no datasets are being tracked."

        lines = [f"Tracked datasets ({len(all_datasets)}):"]

        for dataset in all_datasets:
            dataset_id = dataset['id']
            name = dataset['name']

            history = db.get_dataset_history(db_path, dataset_id, None) if detailed else None
            latest = history[-1] if history and len(history) > 0 else (db.get_latest_version_info(db_path, dataset_id) if not detailed else None)

            if latest is None:
                lines.append(f"  {Fore.YELLOW}? {name} (ID: {dataset_id}) — no versions found{Fore.RESET}")
                continue

            latest_version = float(latest['version'])
            original_path = latest['original_path']

            if not os.path.exists(original_path):
                lines.append(
                    f"\n  — Name: {name} (ID: {dataset_id})  v{latest_version}"
                    f"  {Fore.RED}✗ Not found at {original_path}{Fore.RESET}"
                )
                continue

            live_hash = (
                fu.hash_file(original_path)
                if os.path.isfile(original_path)
                else fu.hash_directory(original_path)
            )

            if live_hash == latest['object_hash']:
                lines.append(
                    f"\n  — Name: {name} (ID: {dataset_id})  Version: {latest_version}"
                    f"  {Fore.GREEN}✔ Up to date{Fore.RESET}"
                )
            else:
                if detailed:
                    matched_version = next(
                        (float(v['version']) for v in history if v['object_hash'] == live_hash),
                        None
                    )
                    if matched_version is not None:
                        lines.append(
                            f"\n  — Name: {name} (ID: {dataset_id})  Version: {latest_version}"
                            f"  {Fore.YELLOW}~ Modified (matches v{matched_version}){Fore.RESET}"
                        )
                    else:
                        lines.append(
                            f"\n  — Name: {name} (ID: {dataset_id}) Version: {latest_version}"
                            f"  {Fore.YELLOW}~ Modified (no version matching the current state found){Fore.RESET}"
                        )
                else:
                    lines.append(
                        f"\n  — Name: {name} (ID: {dataset_id})  Version: {latest_version}"
                        f"  {Fore.YELLOW}~ Modified{Fore.RESET}"
                    )

        return True, "\n".join(lines)
    except sqlite3.Error as e:
        return False, f"Database error: {e}"
    except OSError as e:
        return False, f"Filesystem error: {e}"
    except Exception as e:
        return False, f"An error occurred: {e}"