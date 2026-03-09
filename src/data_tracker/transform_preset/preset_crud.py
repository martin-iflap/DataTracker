import data_tracker.transform_preset.preset_basic as preset_basic
import data_tracker.file_utils as fu
from typing import Tuple
import json
import os



def list_presets(detailed: bool = False) -> Tuple[bool, str]:
    """List and display all available transform presets.
    Args:
        detailed: If True, display full preset configuration for each preset
    Returns:
        Tuple[bool, str]: (success, message)
    """
    tracker_path = fu.find_data_tracker_root()
    if tracker_path is None:
        return False, "Data tracker is not initialized. Please run 'dt init' first."
    try:
        presets_data = preset_basic.load_presets(tracker_path)
        presets = presets_data.get("presets", {})
        if not presets:
            return True, "No transform presets found."

        if not detailed:
            preset_list = "\n".join(f"  - {name}" for name in presets.keys())
            return True, f"Available transform presets ({len(presets)}):\n{preset_list}"

        lines = [f"Available transform presets ({len(presets)}):"]
        for i, (name, cfg) in enumerate(presets.items()):
            lines.append(f"\n  {name}")
            lines.append(f"    Image:      {cfg.get('image', 'N/A')}")
            lines.append(f"    Command:    {cfg.get('command', 'N/A')}")
            lines.append(f"    Message:    {cfg.get('message') or 'N/A'}")
            lines.append(f"    Auto-track: {cfg.get('auto_track', False)}")
            lines.append(f"    No-track:   {cfg.get('no_track', False)}")
            lines.append(f"    Force:      {cfg.get('force', False)}")
            if i < len(presets) - 1:
                lines.append(f"  {'─' * 36}")

        return True, "\n".join(lines)
    except json.JSONDecodeError as e:
        return False, f"JSON error decoding preset configuration: {str(e)}"
    except (FileNotFoundError, ValueError) as e:
        return False, f"Error listing presets: {str(e)}"


def add_preset(name: str, parameters: dict) -> Tuple[bool, str]:
    """Add a new transform preset.
     - Check if a preset with the same name already exists, if so return error message
     - If not, add the new preset to the preset configuration file and return success message
    """
    tracker_path = fu.find_data_tracker_root()
    if tracker_path is None:
        return False, "Data tracker is not initialized. Please run 'dt init' first."
    try:
        data = preset_basic.load_presets(tracker_path)
        presets = data.get("presets", {})
        if name in presets:
            return False, f"Preset '{name}' already exists. Please choose a different name."

        presets[name] = parameters

        preset_path = os.path.join(tracker_path, "presets_config.json")
        with open(preset_path, "w") as f:
            json.dump(data, f, indent=4)

        return True, f"Preset '{name}' has been added successfully."
    except json.JSONDecodeError as e:
        return False, f"JSON decode error while adding preset: {str(e)}"
    except (FileNotFoundError, ValueError) as e:
        return False, f"Error while adding preset: {str(e)}"


def remove_preset(name: str) -> Tuple[bool, str]:
    """Remove and existing transform preset.
     - Check if the preset exists, if not return error message with available presets
     - If it exists, remove it from the config file and return success message
    """
    tracker_path = fu.find_data_tracker_root()
    if tracker_path is None:
        return False, "Data tracker is not initialized. Please run 'dt init' first."
    try:
        data = preset_basic.load_presets(tracker_path)
        presets = data.get("presets", None)
        if presets is None:
            return False, "Preset configuration file is malformed. Missing 'presets' key."

        if name not in presets:
            available = ", ".join(presets.keys()) if presets else "none"
            return False, f"Preset '{name}' not found. Available presets: {available}"

        del presets[name]

        preset_path = os.path.join(tracker_path, "presets_config.json")
        with open(preset_path, "w") as f:
            json.dump(data, f, indent=4)

        return True, f"Preset '{name}' has been removed successfully."
    except json.JSONDecodeError as e:
        return False, f"JSON decode error while removing preset: {str(e)}"
    except (FileNotFoundError, ValueError) as e:
        return False, f"Error while removing preset: {str(e)}"