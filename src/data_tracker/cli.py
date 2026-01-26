import data_tracker.commands as commands
import click

@click.group()
def cli() -> None:
    """Entry point function for the Data Tracker CLI"""
    pass

cli.add_command(commands.init)
cli.add_command(commands.add)
cli.add_command(commands.update)
cli.add_command(commands.remove)
cli.add_command(commands.ls)
cli.add_command(commands.history)
cli.add_command(commands.view)
cli.add_command(commands.compare)
cli.add_command(commands.transform)