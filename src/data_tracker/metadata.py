"""Metadata operations like renaming and changing messages"""
import data_tracker.db_manager as db
import data_tracker.file_utils as fu
from typing import Tuple
import sqlite3
import os

def rename_dataset(id: int, old_name: str, new_name: str) -> Tuple[bool, str]:
    """Renames a dataset and returns the new name"""
    if new_name == old_name:
        return True, f"Dataset name is already '{new_name}'"

    tracker_path = fu.find_data_tracker_root()
    if not tracker_path:
        return False, "Data tracker is not initialized. Please run 'dt init' first."
    db_path = os.path.join(tracker_path, 'tracker.db')

    try:
        with db.open_database(db_path) as conn:
            if not db.dataset_exists(conn, id, old_name):
                return False, f"Specified dataset with ID: {id} and name: '{old_name}' does not exist."

            if db.dataset_exists(conn, None, new_name):
                return False, f"Dataset name '{new_name}' already exists. Please choose another name to avoid conflicts."

            if id is None:
                id = db.get_id_from_name(conn, old_name)
                if id is None:
                    return False, f"Dataset with name '{old_name}' does not exist."

            db.update_dataset_name(conn, id, new_name)
            conn.commit()
            return True, f"Dataset renamed to '{new_name}' successfully."
    except sqlite3.Error as e:
        return False, f"An error occurred while renaming the dataset: {e}"

def change_message(new_message: str,
                   id: int, name: str,
                   provided_version: float|str = None,
                   dataset: bool = False) -> Tuple[bool, str]:
    """Changes the message of a dataset and returns the new message
     - If version is 'latest' calculate the latest version and use that for the update
    """
    tracker_path = fu.find_data_tracker_root()
    if not tracker_path:
        return False, "Data tracker is not initialized. Please run 'dt init' first."
    db_path = os.path.join(tracker_path, 'tracker.db')

    try:
        with (db.open_database(db_path) as conn):
            if not db.dataset_exists(conn, id, name):
                return False, f"Specified dataset with ID: {id} and name: '{name}' does not exist."

            if id is None:
                id = db.get_id_from_name(conn, name)
                if id is None:
                    return False, f"Dataset with name '{name}' does not exist."

            if provided_version == "latest":
                version = db.get_latest_version(conn, id)
                if version is None:
                    return False, f"No versions found for dataset with ID: {id}."
            else:
                version = provided_version

            if dataset:
                rows_updated = db.update_dataset_message(conn, id, new_message)
            else:
                rows_updated = db.update_version_message(conn, id, version, new_message)
            conn.commit()
            if rows_updated == 0:
                return False, f"No dataset found with ID: {id} and version: {version} to update the message."
            if rows_updated == 1:
                return True, f"Dataset message updated successfully to: '{new_message}'"
            else:
                return False, (f"Multiple datasets found with ID: {id} and version: {version}."
                               f" Message update may have affected multiple records.")
    except sqlite3.Error as e:
        return False, f"An error occurred while updating the dataset message: {e}"