import data_tracker.transform_preset.preset_commands as preset_commands
import click

@click.group()
def preset() -> None:
    """Group of commands for managing transformation presets."""
    pass

preset.add_command(preset_commands.ls)
preset.add_command(preset_commands.remove)
preset.add_command(preset_commands.add)