import data_tracker.commands as commands
import click

@click.group()
def cli() -> None:
    """Entry point function for the Data Tracker CLI"""
    pass

cli.add_command(commands.init)
cli.add_command(commands.add)
cli.add_command(commands.add_new_version) # think of a better command name
cli.add_command(commands.remove)
cli.add_command(commands.ls)
cli.add_command(commands.history)