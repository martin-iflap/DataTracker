import data_tracker.transform_preset.preset_basic as preset_basic
import data_tracker.transform_preset.preset_crud as preset_crud
import tempfile
import pytest
import shutil
import json
import os


@pytest.fixture
def temp_config_preset(monkeypatch):
    """Create a temporary tracker directory with a preset config file.
     - monkeypatches find_data_tracker_root to return the temp tracker path
     - no DB or objects/ needed for preset tests
    """
    temp_tracker = tempfile.mkdtemp()
    preset_basic.init_preset(temp_tracker)

    monkeypatch.setattr("data_tracker.file_utils.find_data_tracker_root",
                        lambda: temp_tracker)

    yield temp_tracker

    shutil.rmtree(temp_tracker, ignore_errors=True)


# ------------------------   list_presets -------------------------

def test_list_presets_no_tracker(monkeypatch):
    """list_presets returns failure when no tracker is found."""
    monkeypatch.setattr("data_tracker.file_utils.find_data_tracker_root",
                        lambda: None)
    success, message = preset_crud.list_presets()
    assert success is False
    assert "not initialized" in message


def test_list_presets_empty(temp_config_preset):
    """list_presets returns success with correct message when no presets exist."""
    preset_path = os.path.join(temp_config_preset, "presets_config.json")
    with open(preset_path, "w") as f:
        json.dump({"presets": {}, "schema_version": "1.0"}, f)

    success, message = preset_crud.list_presets()
    assert success is True
    assert "No transform presets found" in message


def test_list_presets_simple(temp_config_preset):
    """list_presets returns preset names in normal (non-detailed) mode."""
    success, message = preset_crud.list_presets(detailed=False)
    assert success is True
    assert "example-python" in message


def test_list_presets_detailed_two_presets(temp_config_preset):
    """list_presets returns full preset fields in detailed mode with correct styling."""
    preset_path = os.path.join(temp_config_preset, "presets_config.json")
    with open(preset_path, "w") as f:
        json.dump({
            "presets": {
                "example-python": {
                    "image": "python:3.11-slim",
                    "command": "python /input/script.py --output /output/result.csv",
                    "auto_track": False,
                    "no_track": False,
                    "message": "Example python transformation",
                    "force": False,
                },
                "example-r-script": {
                    "image": "r-base:latest",
                    "command": "Rscript /input/script.R /output/result.csv",
                    "auto_track": True,
                    "no_track": False,
                    "message": "Example R transformation",
                    "force": True,
                }
            },
            "schema_version": "1.0"
        }, f)

    success, message = preset_crud.list_presets(detailed=True)

    assert success is True
    assert "example-python" in message
    assert "python:3.11-slim" in message
    assert "Image:" in message
    assert "Command:" in message
    assert "Auto-track:" in message
    assert "Force:      False" in message

    assert "example-r-script" in message
    assert "r-base:latest" in message
    assert "Rscript /input/script.R /output/result.csv" in message
    assert "Auto-track: True" in message
    assert "Force:      True" in message
    assert "─" in message  # check for separators

# -------------------------   add_preset   --------------------------------

def test_add_preset_no_tracker(monkeypatch):
    """add_preset returns failure when no tracker is found."""
    monkeypatch.setattr("data_tracker.file_utils.find_data_tracker_root",
                        lambda: None)
    success, message = preset_crud.add_preset("new-preset", {"image": "python:3.11-slim", "command": "echo test"})
    assert success is False
    assert "not initialized" in message


def test_add_preset_success(temp_config_preset):
    """add_preset adds a new preset to the config file and returns success."""
    params = {
        "image": "python:3.11-slim",
        "command": "python /input/run.py",
        "force": False,
        "auto_track": False,
        "no_track": False,
        "message": None,
    }
    success, message = preset_crud.add_preset("new-preset", params)
    assert success is True
    assert "new-preset" in message

    preset_path = os.path.join(temp_config_preset, "presets_config.json")
    with open(preset_path, "r") as f:
        data = json.load(f)
    assert "new-preset" in data["presets"]
    assert data["presets"]["new-preset"]["image"] == "python:3.11-slim"


def test_add_preset_duplicate(temp_config_preset):
    """add_preset returns failure when a preset with the same name already exists."""
    params = {"image": "python:3.11-slim", "command": "echo test"}
    success, message = preset_crud.add_preset("example-python", params)
    assert success is False
    assert "already exists" in message

# ---------------------   remove_preset   ----------------------------

def test_remove_preset_no_tracker(monkeypatch):
    """remove_preset returns failure when no tracker is found."""
    monkeypatch.setattr("data_tracker.file_utils.find_data_tracker_root",
                        lambda: None)
    success, message = preset_crud.remove_preset("example-python")
    assert success is False
    assert "not initialized" in message


def test_remove_preset_success(temp_config_preset):
    """remove_preset removes the preset from the config file and returns success."""
    success, message = preset_crud.remove_preset("example-python")
    assert success is True
    assert "example-python" in message

    preset_path = os.path.join(temp_config_preset, "presets_config.json")
    with open(preset_path, "r") as f:
        data = json.load(f)
    assert "example-python" not in data["presets"]


def test_remove_preset_not_found(temp_config_preset):
    """remove_preset returns failure with available presets when name does not exist."""
    success, message = preset_crud.remove_preset("nonexistent-preset")
    assert success is False
    assert "nonexistent-preset" in message
    assert "not found" in message
    assert "example-python" in message  # listed as available preset


def test_remove_preset_presets_key_missing(temp_config_preset):
    """remove_preset returns failure when presets key is missing from config file."""
    preset_path = os.path.join(temp_config_preset, "presets_config.json")
    with open(preset_path, "w") as f:
        json.dump({"schema_version": "1.0"}, f)

    success, message = preset_crud.remove_preset("example-python")
    assert success is False
    assert "file is malformed" in message