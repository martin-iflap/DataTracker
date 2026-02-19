import json
import os


def init_preset(tracker_path: str) -> None:
    """Initialize the preset configuration file with a default example.
    Args:
        tracker_path: Path to .data_tracker directory
    """
    preset_template = {
        "presets": {
            "example-python": {
                "image": "python:3.11-slim",
                "command": "python /input/script.py --output /output/result.csv",
                "auto_track": True,
                "message": "Example python transformation",
                "force": False,
            }
        },
        "schema_version": "1.0"
    }
    preset_path = os.path.join(tracker_path, "presets_config.json")
    with open(preset_path, "w") as f:
        json.dump(preset_template, f, indent=4)

def load_presets(tracker_path: str) -> dict:
    """Load the preset configuration from the JSON file and return as dict.
    Args:
        tracker_path: Path to .data_tracker directory
    Returns:
        dict: Preset configuration data
    """
    preset_path = os.path.join(tracker_path, "presets_config.json")
    if not os.path.exists(preset_path):
        raise FileNotFoundError(f"Preset configuration file not found at {preset_path}. Please run init_preset first.")
    with open(preset_path, "r") as f:
        presets = json.load(f)
    return presets

def preset_exists(tracker_path: str, preset_name: str) -> bool:
    """Check if a preset with the given name exists in config file.
    Args:
        tracker_path: Path to .data_tracker directory
        preset_name: Name of the preset to check
    Returns:
        bool: True if preset exists, False otherwise
    """
    try:
        presets_data = load_presets(tracker_path)
        return preset_name in presets_data.get("presets", {})
    except (FileNotFoundError, json.JSONDecodeError):
        return False

def get_preset(tracker_path: str, preset_name: str) -> dict:
    """Retrieve a specific preset by name.
    Args:
        tracker_path: Path to .data_tracker directory
        preset_name: Name of the preset to retrieve
    Returns:
        dict: Preset configuration
    Raises:
        ValueError: If preset doesn't exist
        FileNotFoundError: If preset file doesn't exist
        json.JSONDecodeError: If preset file is malformed
    """
    presets_data = load_presets(tracker_path)
    presets = presets_data.get("presets", {})

    if preset_name not in presets:
        available = ", ".join(presets.keys()) if presets else "none"
        raise ValueError(f"Preset '{preset_name}' not found. Available presets: {available}")

    return presets[preset_name]
