"""Business logic for transform command - keeps commands.py thin"""
import data_tracker.docker_manager as docker_m
import data_tracker.transform_preset as tp
import data_tracker.file_utils as fu
import data_tracker.db_manager as db
from typing import Tuple, Optional
import data_tracker.core as core
import os

def execute_transform(
    db_path: str,
    tracker_path: str,
    preset_name: Optional[str],
    image: Optional[str],
    input_data: str,
    output_data: str,
    command: Optional[str],
    force: bool,
    auto_track: bool,
    no_track: bool,
    dataset_id: Optional[int],
    message: Optional[str],
    version: Optional[float]
) -> Tuple[bool, str, dict]:
    """Execute transformation with auto-versioning logic.

    Args:
        db_path: Path to tracker database
        tracker_path: Path to .data_tracker directory
        preset_name: Name of preset to use (if any)
        image: Docker image (CLI overrides preset)
        input_data: Input path (CLI overrides preset)
        output_data: Output path (CLI overrides preset)
        command: Transform command (CLI overrides preset)
        force: Force flag (CLI overrides preset)
        auto_track: Auto-track flag (CLI overrides preset)
        no_track: No-track flag (CLI overrides preset)
        dataset_id: Dataset ID for explicit tracking
        message: Custom message (CLI overrides preset)
        version: Custom version number

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

    # Load preset and apply override hierarchy: CLI > preset > defaults
    if preset_name:
        try:
            preset_data = tp.get_preset(tracker_path, preset_name)

            # Apply overrides: CLI values take precedence over preset values
            image = image if image is not None else preset_data.get('image', False)
            command = command if command is not None else preset_data.get('command', False)

            # For boolean flags, only override with preset if CLI left the default False
            if not force:
                force = preset_data.get('force', False)
            if not auto_track:
                auto_track = preset_data.get('auto_track', False)
            if not no_track:
                no_track = preset_data.get('no_track', False)

            if message is None:
                message = preset_data.get('message')

            # Validate that required fields are now populated
            if not all([image, input_data, output_data, command]):
                missing = []
                if not image: missing.append('image')
                if not input_data: missing.append('input-data')
                if not output_data: missing.append('output-data')
                if not command: missing.append('command')
                return False, (
                    f"Preset '{preset_name}' is missing required fields: {', '.join(missing)}\n"
                    f"Provide these via CLI options or update the preset configuration."
                ), metadata

        except ValueError as e:
            return False, str(e), metadata
        except Exception as e:
            return False, f"Failed to load preset '{preset_name}': {e}", metadata

    # Resolve paths to absolute so DB lookups and Docker mounts are always consistent
    input_data = os.path.abspath(input_data)
    output_data = os.path.abspath(output_data)

    try:
        if dataset_id is not None:
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
                f"Input path: {input_data}"
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
                if version is not None:
                    if not db.check_version_exists(conn, found_dataset_id, version):
                        new_version = version
                    else:
                        status_msg += (f"\nProvided --version {version} is not valid for dataset ID {found_dataset_id}."
                                       f"\nOutput not versioned. To version manually, run:"
                                       f"\ndt update {output_data} --id {found_dataset_id} -v <VERSION>"
                                       f"\nSuggested next version: {round(latest_version + 0.1, 1)}")
                        return True, status_msg, metadata
                else:
                    new_version = round(latest_version + 0.1, 1)
        except Exception as e:
            status_msg += (f"\nTransform succeeded but version calculation failed: {e}"
                           f"\nOutput not versioned. To version manually, run:"
                           f"\ndt update {output_data} --id {found_dataset_id} -v <VERSION>")
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

    status_msg += f"\nOutput written to: {output_data}"
    return True, status_msg, metadata

def validate_transform_environment() -> Tuple[bool, str]:
    """Validate Docker and tracker are ready for transform command
    Returns: (success, tracker_path_or_error_message)
    """
    if not docker_m.is_docker_installed():
        return False, (
            "Docker is not installed or not found in PATH.\n\n"
            "To use the transform command, you need Docker:\n"
            "  • Windows/Mac: Install Docker Desktop from docker.com\n"
            "  • Linux: Install Docker Engine using your package manager\n\n"
            "After installation, verify with: docker --version"
        )

    tracker_path = fu.find_data_tracker_root()
    if not tracker_path:
        return False, (
            "Data tracker not initialized in this directory.\n\n"
            "Run 'dt init' first to initialize tracking, then try again."
        )

    return True, tracker_path