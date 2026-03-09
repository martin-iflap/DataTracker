import data_tracker.transform_preset.preset_crud as crud
import click
import sys


@click.command()
@click.option("-d", "--detailed", is_flag=True, default=False,
              help="Show detailed information for each preset")
def ls(detailed: bool) -> None:
    """List all available transform presets."""
    try:
        success, message = crud.list_presets(detailed=detailed)
        if success:
            click.echo(message)
        else:
            click.echo(f"Error: {message}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True, color="red")
        sys.exit(1)


@click.command()
@click.argument("name")
@click.option("-i", "--image", required=True,
              help="Docker image to use for the transformation")
@click.option("-c", "--command", required=True,
              help="Transformation command (overrides preset)")
@click.option("-f", "--force", is_flag=True, default=False,
              help="Force execution without command validation")
@click.option("--auto-track", is_flag=True, default=False,
              help="Auto-add input if not tracked, then version output")
@click.option("--no-track", is_flag=True, default=False,
              help="Skip versioning even if input is tracked")
@click.option("-m", "--message", default=None,
              help="Optional message describing the preset")
def add(name: str, image: str, command: str, force: bool,
        auto_track: bool, no_track: bool, message: str) -> None:
    """Add a new transform preset."""
    try:
        if auto_track and no_track:
            raise click.UsageError("Cannot use --auto-track and --no-track together")

        parameters = {
            "image": image,
            "command": command,
            "auto_track": auto_track,
            "no_track": no_track,
            "message": message,
            "force": force
        }
        success, result_message = crud.add_preset(name, parameters)
        if success:
            click.echo(result_message)
        else:
            click.echo(f"Error: {result_message}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True, color="red")
        sys.exit(1)


@click.command()
@click.argument("name")
def remove(name: str) -> None:
    """Remove an existing transform preset."""
    try:
        confirm_msg = f"Are you sure you want to remove preset: {name}? This action cannot be undone."
        click.confirm(confirm_msg, abort=True)

        success, message = crud.remove_preset(name)
        if success:
            click.echo(message)
        else:
            click.echo(f"Error: {message}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True, color="red")
        sys.exit(1)