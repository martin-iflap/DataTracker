import data_tracker.docker_manager as docker_m
import data_tracker.comparison as comparison
import data_tracker.file_utils as fu
import data_tracker.core as core
import click
import sys

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
@click.argument("v1", type=float, default=1.0)
@click.argument("v2", type=float, default=123.123) # keep an eye on the defaults
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
@click.option("--id", type=int, default=None, help="ID of the dataset to export") # make id and name into one argument?
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
@click.option("--image", required=True, help="Path to the image")
@click.option("-in", "--input-data", required=True, help="Path to the input data") # is the shadowing a problem?
@click.option("-out", "--output-data", required=True, help="Path to the output data")
@click.option("--command", required=True, help="Transformation command to apply use mounted /input and /output")
@click.option("-f", "--force", is_flag=True, default=False, help="Force execution without command validation")
def transform(image: str, input_data: str, output_data: str, command: str, force: bool) -> None:
    """Apply a transformation to the data using a containerized environment"""
    try:
        if not docker_m.is_docker_installed():
            raise EnvironmentError("Docker is not installed or not found in PATH.")
        success, message = docker_m.transform_data(image, input_data, output_data, command, force)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)



# create get_db_path function? add validate version function?
# add export command for exporting and restoring datasets
# add some tests