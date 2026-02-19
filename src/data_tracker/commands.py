import data_tracker.comparison as comparison
import data_tracker.metadata as metadata
import data_tracker.file_utils as fu
import data_tracker.transform as tf
import data_tracker.core as core
import click
import sys
import os

@click.command()
def init() -> None:
    """Initialize the data tracker"""
    try:
        success, message = core.initialize_tracker()
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="yellow")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.argument("data_path")
@click.option("--title", default=None, help="Title of the dataset")
@click.option("-v", "--version", default=1.0, type=float, help="Version number of the data being added")
@click.option("-m", "--message", default=None, help="Additional message about the data")
def add(data_path: str, title: str, version: float, message: str) -> None:
    """Add new data to the tracker"""
    try:
        success, message = core.add_data(data_path, title, version, message)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.argument("data_path")
@click.option("--id", type=int, default=None, help="ID of the dataset to update")
@click.option("--name", default=None, help="Name of the dataset to update")
@click.option("-v", "--version", type=float, default=None, help="Version number")
@click.option("-m", "--message", default=None, help="Message describing the update")
def update(data_path: str, id: int, name: str, version: float, message: str) -> None:
    """Add a new version of existing dataset to the tracker"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")

    try:
        success, result_message = core.update_data(data_path, id, name, version, message)
        if success:
            click.echo(result_message)
        else:
            click.secho(result_message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.option("--id", type=int, default=None, help="ID of the dataset to remove")
@click.option("--name", default=None, help="Name of the dataset to remove")
def remove(id: int, name: str) -> None:
    """Remove data from the tracker"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")

    try:
        success, message = core.remove_data(id, name)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.option("-s", "--structure", is_flag=True, help="Display the structure of tracked data files")
def ls(structure: bool) -> None:
    """List all tracked data files"""
    try:
        success, message = core.list_data(structure)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.option("--id", type=int, default=None, help="ID of the dataset")
@click.option("--name", default=None, help="Name of the dataset")
@click.option("-d", "--detailed", is_flag=True, help="Show detailed history with file changes")
def history(id: int, name: str, detailed: bool) -> None:
    """Show history of changes for a specific data file"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")
    try:
        success, message = core.get_history(id, name, detailed)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.option("-v", "version", type=float, required=True, help="Version number of the dataset to view")
@click.option("--id", type=int, default=None, help="ID of the dataset")
@click.option("--name", default=None, help="Name of the dataset")
def view(id: int, name: str, version: float) -> None:
    """Open a specific version of a dataset"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")
    try:
        success, message = fu.open_dataset_version(id, name, version)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.argument("v1", type=float, default=None)
@click.argument("v2", type=float, default=None)
@click.option("--id", type=int, default=None, help="ID of the dataset")
@click.option("--name", default=None, help="Name of the dataset")
def compare(id: int, name: str, v1: float, v2: float) -> None:
    """Compare two versions of a dataset and show differences"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")
    try:
        success, message = comparison.compare_dataset_versions(id, name, v1, v2)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.argument("export_path")
@click.option("--id", type=int, default=None, help="ID of the dataset to export")
@click.option("--name", default=None, help="Name of the dataset to export")
@click.option("-v", "--version", type=float, required=True, help="Version of the dataset to export")
@click.option("-f", "--force", is_flag=True, default=None, help="Overwrite existing files at the export location")
@click.option("-r", "--preserve-root", is_flag=True, default=None, help="Preserve the root dataset directory name")
def export(export_path: str, id: int, name: str,
           version: float, force: bool, preserve_root: bool) -> None:
    """Export a specific version of a dataset to a specified location"""
    try:
        if bool(id) == bool(name):
            raise click.UsageError("Provide exactly one of --id or --name")

        success, message = fu.export_file(export_path, id, name, version, force, preserve_root)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.option("-p", "--preset", default=None, help="Use a named transform preset")
@click.option("-i", "--image", default=None, help="Docker image (overrides preset)")
@click.option("-in", "--input-data", default=None,
              help="Path to the input data (overrides preset)")
@click.option("-out", "--output-data", default=None,
              help="Path to the output data (overrides preset)")
@click.option("-c", "--command", default=None,
              help="Transformation command (overrides preset)")
@click.option("-f", "--force", is_flag=True, default=None,
              help="Force execution without command validation")
@click.option("--auto-track", is_flag=True, default=None,
              help="Auto-add input if not tracked, then version output")
@click.option("--no-track", is_flag=True, default=None,
              help="Skip versioning even if input is tracked")
@click.option("-id", "--dataset-id", type=int, default=None,
              help="Explicitly specify which dataset to update (advanced)")
@click.option("-v", "--version", type=float, default=None,
              help="Manually specify version number for auto-versioning (advanced)")
@click.option("-m", "--message", default=None,
              help="Custom message for auto-versioned output")
def transform(preset: str, image: str, input_data: str, output_data: str,
              command: str, force: bool, auto_track: bool,
              no_track: bool, dataset_id: int, message: str, version: float) -> None:
    """Apply a transformation to the data using a containerized environment
     - Can use presets for common transformations or specify all options manually.
     - CLI options override preset values when both are provided.
    """
    try:
        if auto_track and no_track:
            raise click.UsageError("Cannot use --auto-track and --no-track together")

        success, result = tf.validate_transform_environment()
        if not success:
            click.secho(result, fg="red")
            sys.exit(1)

        tracker_path = result
        db_path = os.path.join(tracker_path, "tracker.db")

        if preset: # validate preset existence if specified
            import data_tracker.transform_preset as tp               # import here?
            if not tp.preset_exists(tracker_path, preset):
                click.secho(f"Error: Preset '{preset}' not found", fg="red")
                click.secho("Run 'dt preset list' to see available presets (coming soon)", fg="yellow")
                sys.exit(1)

        if not preset:
            if not all([image, input_data, output_data, command]): # validate all required options are provided
                raise click.UsageError(
                    "When not using --preset, all of the following are required:\n"
                    "  --image, --input-data, --output-data, --command"
                )

        if message:
            message = message.strip()

        # execute the transformation with all the arguments
        success, message, transform_metadata = tf.execute_transform(
            db_path, tracker_path, preset, image, input_data, output_data, command,
            force, auto_track, no_track, dataset_id, message, version
        )

        if success:
            click.echo(message)
            if transform_metadata['tracked']:
                click.secho(
                    f"Versioned: {transform_metadata['old_version']} → {transform_metadata['new_version']}",
                    fg="green"
                )
        else:
            click.secho(message, fg="red")
            sys.exit(1)

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
def storage() -> None:
    """Show statistics about the tracked datasets
     - show the number of objects and total size
    """
    try:
        success, message = fu.get_storage_stats()
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.argument("new_name")
@click.option("--id", required=False, type=int, help="ID of the dataset to rename")
@click.option("-n", "--name", required=False, help="Old name of the dataset to rename")
def rename(new_name: str, id: int, name: str) -> None:
    """Rename a dataset specified by the dataset ID or name"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")
    try:
        success, message = metadata.rename_dataset(id, name, new_name)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.argument("new_message")
@click.option("--id", required=False, type=int, help="ID of the dataset to update message for")
@click.option("-n", "--name", required=False, help="Name of the dataset to update message for")
@click.option("-v", "--version", required=False, type=float, help="Version number to update message for")
@click.option("--latest", is_flag=True, help="Update message for the latest version")
@click.option("--dataset", is_flag=True, help="Update the dataset message")
def annotate(new_message: str, id: int, name: str, version: float, latest: bool, dataset: bool) -> None:
    """Update the message for a specific dataset version
    - specify dataset by id or name, version number and new message
    """
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")

    choices = [version is not None, latest, dataset]
    if sum(choices) == 0:
        raise click.UsageError("Specify target: --version X.X, --latest, or --dataset")
    if sum(choices) > 1:
        raise click.UsageError("Provide only one of --version, --latest, or --dataset")

    try:
        if dataset:
            success, message = metadata.change_message(new_message, id, name, dataset=dataset)
        else:
            target_version = "latest" if latest else version
            success, message = metadata.change_message(new_message, id, name,
                                                       provided_version=target_version)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)




# dataset tagging (like git tags)
# difference previewing before update command
# batch file operations like export all
# tests


# if user specifies something different from the preset use cli as default
# commands to manage presets like add/remove/list
# keep an eye on the relative paths