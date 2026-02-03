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
    output_data: str,
    command: str,
    force: bool,
    auto_track: bool,
    no_track: bool,
    dataset_id: int,
    message: str,
    version: float
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
    try:
        if dataset_id:
            found_dataset_id = dataset_id
            dataset_name = db.get_dataset_name_from_id(db_path, dataset_id)
            if not dataset_name:
                return False, f"Dataset with ID {dataset_id} does not exist", metadata
        else:
            found_dataset_id = db.find_dataset_by_path(db_path, input_data)
            dataset_name = db.get_dataset_name_from_id(db_path, found_dataset_id) if found_dataset_id else None
    except ValueError as e:
        return False, f"Dataset lookup failed: {e}\nRun 'dt ls' to see available datasets", metadata
    except Exception as e:
        return False, f"Database error while checking dataset: {e}", metadata

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
        success, msg = core.add_data(
            input_data, title=None,
            version=1.0, message=message or "Auto-added for transform"
        )
        if not success:
            return False, f"Failed to add input: {msg}", metadata
        try:
            found_dataset_id = db.find_dataset_by_path(db_path, input_data)
            dataset_name = db.get_dataset_name_from_id(db_path, found_dataset_id)
            status_msg += f"\nAdded as '{dataset_name}' (ID: {found_dataset_id})"
        except ValueError as e:
            return False, (
                f"Critical: Dataset added but lookup failed: {e}\n\n"
                f"Your database may be corrupted. Run:\n"
                f"  dt ls  # Check for orphaned dataset"
            ), metadata
        except Exception as e:
            return False, (
                f"Critical: Dataset added but database lookup failed: {e}\n\n"
                f"Your database may be corrupted. Run:\n"
                f"  dt ls  # Check for orphaned dataset\n"
                f"  dt remove --id <ID>  # Remove it if found\n"
                f"Input path: {os.path.abspath(input_data)}"
            ), metadata

    success, transform_msg = docker_m.transform_data( # run transformation
        image, input_data, output_data, command, force)
    if not success:
        if found_dataset_id and should_add_input:
            try:
                core.remove_data(found_dataset_id, None)
                return False, (f"Transformation failed: {transform_msg}\n"
                               f"Rolled back: Removed auto-added dataset '{dataset_name}'"), metadata
            except Exception as e:
                return False, (f"Transformation failed: {transform_msg}\n"
                               f"Rollback failed: {e}\n"
                               f"Failed to remove dataset '{dataset_name}' ID: {found_dataset_id}"
                               f"Manually run: dt remove --id {found_dataset_id}"), metadata
        else:
            return False, transform_msg, metadata

    if should_track and found_dataset_id: # track output
        try:
            with db.open_database(db_path) as conn:
                latest_version = db.get_latest_version(conn, found_dataset_id)
                if version:
                    if not db.check_version_exists(conn, found_dataset_id, version):
                        new_version = version
                    else:
                        status_msg += (f"\nProvided --version {version} is not valid for dataset ID {found_dataset_id}."
                                       f"\nOutput not versioned. To version manually, run:"
                                       f"\ndt update {os.path.abspath(output_data)} --id {found_dataset_id} -v <VERSION>"
                                       f"\nSuggested next version: {round(latest_version + 0.1, 1)}")
                        return True, status_msg, metadata
                else:
                    new_version = round(latest_version + 0.1, 1)
        except Exception as e:
            status_msg += (f"\nTransform succeeded but version calculation failed: {e}"
                           f"\nOutput not versioned. To version manually, run:"
                           f"\ndt update {os.path.abspath(output_data)} --id {found_dataset_id} -v <VERSION>")
            return True, status_msg, metadata

        success, update_msg = core.update_data(
            output_data, data_id=found_dataset_id,
            name=None, version=new_version,
            message=message or f"Transformed with {image}"
        )

        if success:
            metadata['dataset_name'] = dataset_name
            metadata['dataset_id'] = found_dataset_id
            metadata['old_version'] = latest_version
            metadata['new_version'] = new_version
            metadata['tracked'] = True
            status_msg += f"\nUpdated '{dataset_name}' to version {new_version}"
        else:
            status_msg += (f"\nTransform succeeded but versioning failed: {update_msg}"
                           f"\nRun dt update --id {found_dataset_id} to version manually")

    output_abs = os.path.abspath(output_data)
    status_msg += f"\nOutput written to: {output_abs}"
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