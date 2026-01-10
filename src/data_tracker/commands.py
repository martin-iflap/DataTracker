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
@click.option("--title", default=None, help="Name of the dataset")
@click.option("--version", default=1, type=int, help="Version number of the data being added")
def add(data_path: str, title: str, version: int) -> None:
    """Add new data to the tracker"""
    try:
        success, message = core.add_data(data_path, title, version)
        if success:
            click.echo(message)
        else:
            click.secho(message, fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

@click.command()
def add_new_version() -> None:
    """Add a new version of existing dataset to the tracker"""
    raise NotImplementedError("command not implemented yet")

@click.command()
@click.option("--id", type=int)
@click.option("--name")
def remove(id: int = None, name: str = None) -> None:
    """Remove data from the tracker"""
    click.echo("removing data")

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
@click.option("--id", type=int)
@click.option("--name")
def history(id: int = None, name: str = None) -> None:
    """Show history of changes for a specific data file"""
    if bool(id) == bool(name):
        raise click.UsageError("Provide exactly one of --id or --name")



# Handle all the possible add data cases like data already added but now with different name...
# Keep an eye on the error handling and messages to the user and perhaps add logging
# Consider edge cases like adding directories, large files, unsupported file types etc.