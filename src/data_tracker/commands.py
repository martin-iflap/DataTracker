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
def ls() -> None:
    """List all tracked data files"""
    try:
        success, message = core.list_data()
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.option("--id", type=int, default=None, help="ID of the dataset") # should I display the hash?
@click.option("--name", default=None, help="Name of the dataset")
def history(id: int, name: str) -> None:
    """Show history of changes for a specific data file"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")
    try:
        success, message = core.get_history(id, name)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
@click.option("-v", "version", type=float) # change this
@click.option("--id", type=int, default=None, help="ID of the dataset")
@click.option("--name", default=None, help="Name of the dataset")
def view(id: int, name: str, version: float) -> None:
    """Open a specific version of a dataset"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")
    if not version:
        raise click.UsageError("Version number must be provided with -v/--version")
    try:
        success, message = core.open_dataset_version(id, name, version)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


# do not allow updating unchanged data, create get_db_path function?

# add compare dataset versions
# For single files:
    # File hash comparison: Quick check if files are identical
    # Size difference: How much the file grew/shrunk
    # Line count difference (for text files): Added/removed/changed lines
    # Content similarity percentage: Using difflib or similar
    # File type change: If extension changed
# For directories:
    # Structure changes: Files added, removed, or renamed
    # File-by-file comparison: Show which files changed and how
    # Total size difference: Overall storage impact
    # Summary statistics: Total files added/removed/modified