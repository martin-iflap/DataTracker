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
@click.argument("data_id")
def remove() -> None:
    """Remove data from the tracker"""
    click.echo("removing data")

@click.command()
def list_data() -> None: # to avoid conflict with built-in list
    """List all tracked data files"""
    click.echo("listing data")

@click.command()
@click.argument("data_id") #or name that matches to id
def history(data_id: str) -> None:
    """Show history of changes for a specific data file"""
    click.echo(f"showing history for data id: {data_id}")