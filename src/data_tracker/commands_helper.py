"""Business logic for CLI commands - keeps commands.py thin"""
import data_tracker.docker_manager as docker_m
import data_tracker.file_utils as fu
import data_tracker.db_manager as db
import data_tracker.core as core
from typing import Tuple
import os

def execute_transform(
    db_path: str,
    image: str,
    input_data: str,
    output_data: str, # check edge case scenarios like output == input
    command: str, # check edge cases like empty command
    force: bool, # check edge cases like added new dataset but transform fails
    auto_track: bool, # cover better database function errors
    no_track: bool,
    dataset_id: int,
    message: str
) -> Tuple[bool, str, dict]:
    """Execute transformation with auto-versioning logic.
    Returns: (success, message, metadata_dict)
    metadata_dict contains:
        - dataset_name: str | None
        - dataset_id: int | None
        - old_version: float | None
        - new_version: float | None
        - tracked: bool
    """
    metadata = {
        'dataset_name': None,
        'dataset_id': None,
        'old_version': None,
        'new_version': None,
        'tracked': False
    }
    if dataset_id:
        found_dataset_id = dataset_id
        dataset_name = db.get_dataset_name_from_id(db_path, dataset_id)
        if not dataset_name:
            return False, f"Dataset with ID {dataset_id} does not exist", metadata
    else:
        found_dataset_id = db.find_dataset_by_path(db_path, input_data)
        dataset_name = db.get_dataset_name_from_id(db_path, found_dataset_id) if found_dataset_id else None

    should_track = False
    should_add_input = False
    status_msg = ""

    if no_track: # Explicit no-track
        should_track = False
        status_msg = "Versioning disabled (--no-track)"
    elif found_dataset_id: # Input tracked -> track output
        should_track = True
        status_msg = f"Input recognized as dataset '{dataset_name}' (ID: {found_dataset_id})"
    elif auto_track: # Input not tracked -> add and track
        should_add_input = True
        should_track = True
        status_msg = "Input not tracked, will add as new dataset (--auto-track)"
    else: # Input not tracked -> no track
        should_track = False
        status_msg = (
            "Input not tracked (output will not be versioned)\n"
            "   Tip: Use --auto-track to version both input and output"
        )

    if should_add_input: # Add input as new dataset
        success, msg = core.add_data(input_data, title=None,
            version=1.0, message=message or "Auto-added for transform") # what if message is empty string?
        if not success:
            return False, f"Failed to add input: {msg}", metadata

        found_dataset_id = db.find_dataset_by_path(db_path, input_data)
        dataset_name = db.get_dataset_name_from_id(db_path, found_dataset_id)
        status_msg += f"\nAdded as '{dataset_name}' (ID: {found_dataset_id})"

    success, transform_msg = docker_m.transform_data( # run transformation
        image, input_data, output_data, command, force
    )
    if not success:
        return False, transform_msg, metadata

    if should_track and found_dataset_id: # track output
        latest_version = db.get_latest_version(db.open_database(db_path), found_dataset_id)
        new_version = round(latest_version + 0.1, 1) # take a look at this logic later

        success, update_msg = core.update_data(
            output_data, data_id=found_dataset_id,
            name=None, version=new_version,
            message=message or f"Transformed with {image}" # what if message is empty string?
        )

        if success:
            metadata['dataset_name'] = dataset_name
            metadata['dataset_id'] = found_dataset_id
            metadata['old_version'] = latest_version
            metadata['new_version'] = new_version
            metadata['tracked'] = True
            status_msg += f"\nUpdated '{dataset_name}' to version {new_version}"
        else:
            status_msg += f"\nTransform succeeded but versioning failed: {update_msg}"

    output_abs = os.path.abspath(output_data)
    status_msg += f"\n→ Output written to: {output_abs}"
    return True, status_msg, metadata

def validate_transform_environment() -> Tuple[bool, str]:
    """Validate Docker and tracker are ready for transform command
    Returns: (success, error_message)
    """
    if not docker_m.is_docker_installed():
        return False, "Docker is not installed or not found in PATH"
    tracker_path = fu.find_data_tracker_root()
    if not tracker_path:
        return False, "Data tracker not initialized. Run 'dt init' first"

    return True, tracker_path