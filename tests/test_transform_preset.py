from data_tracker import transform_preset as preset
import shutil
import json
import pytest
import re



def test_preset_initialization(tmp_path):
    """Test that the preset initialization creates the expected configuration file with correct content."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()

    preset.init_preset(fake_tracker_path)

    preset_file = fake_tracker_path / "presets_config.json"
    assert preset_file.exists(), "Preset configuration file was not created."
    with open(preset_file, "r") as f:
        data = json.load(f)
    assert "presets" in data, "Preset configuration file does not contain 'presets' key."
    assert "example-python" in data["presets"], "Example transformation preset is missing from configuration."
    assert data["presets"]["example-python"]["image"] == "python:3.11-slim", "Preset image does not match expected value."
    assert data["presets"]["example-python"]["command"] == "python /input/script.py --output /output/result.csv", "Preset command does not match expected value."
    assert data["presets"]["example-python"]["auto_track"] is True, "Preset auto_track does not match expected value."
    assert data["presets"]["example-python"]["message"] == "Example python transformation", "Preset message does not match expected value."

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise

def test_load_presets_with_valid_file(tmp_path):
    """Test that loading presets from a valid configuration file returns the expected data structure."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()
    preset.init_preset(fake_tracker_path)

    presets_data = preset.load_presets(fake_tracker_path)
    assert isinstance(presets_data, dict), "Loaded presets data is not a dictionary."
    assert "presets" in presets_data, "Loaded presets data does not contain 'presets' key."
    assert "example-python" in presets_data["presets"], "Example transformation preset is missing from loaded data."

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise

def test_load_presets_with_missing_file(tmp_path):
    """Test that loading presets from a non-existent configuration file raises FileNotFoundError."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()

    expected_path = str(fake_tracker_path / "presets_config.json")
    with pytest.raises(FileNotFoundError, match=re.escape(f"Preset configuration file not found at {expected_path}")):
        preset.load_presets(fake_tracker_path)

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise

def test_preset_exists(tmp_path):
    """Test that the preset_exists function correctly identifies existing and non-existing presets."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()
    preset.init_preset(fake_tracker_path)

    assert preset.preset_exists(fake_tracker_path, "example-python") is True, "preset_exists did not find existing preset."
    assert preset.preset_exists(fake_tracker_path, "nonexistent-preset") is False, "preset_exists incorrectly found non-existing preset."

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise

def preset_exists_with_malformed_file(tmp_path):
    """Test that preset_exists returns False when the preset configuration file is malformed."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()
    preset_file = fake_tracker_path / "presets_config.json"
    with open(preset_file, "w") as f:
        f.write("This is not valid JSON")

    assert preset.preset_exists(fake_tracker_path, "example-python") is False, "preset_exists should return False when config file is malformed."

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise

def test_preset_exists_with_missing_file(tmp_path):
    """Test that preset_exists returns False when the preset configuration file is missing."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()

    assert preset.preset_exists(fake_tracker_path, "example-python") is False, "preset_exists should return False when config file is missing."

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise

def test_get_preset(tmp_path):
    """Test that the get_preset function retrieves the correct preset configuration."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()
    preset.init_preset(fake_tracker_path)

    preset_config = preset.get_preset(fake_tracker_path, "example-python")
    assert isinstance(preset_config, dict), "get_preset did not return a dictionary."
    assert preset_config["image"] == "python:3.11-slim", "Preset image does not match expected value."
    assert preset_config["command"] == "python /input/script.py --output /output/result.csv", "Preset command does not match expected value."
    assert preset_config["auto_track"] is True, "Preset auto_track does not match expected value."
    assert preset_config["message"] == "Example python transformation", "Preset message does not match expected value."

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise

def test_get_preset_with_nonexistent_preset(tmp_path):
    """Test that get_preset raises ValueError when trying to retrieve a non-existent preset."""
    fake_tracker_path = tmp_path / ".data_tracker"
    fake_tracker_path.mkdir()
    preset.init_preset(fake_tracker_path)

    with pytest.raises(ValueError, match=re.escape("Preset 'nonexistent-preset' not found.")):
        preset.get_preset(fake_tracker_path, "nonexistent-preset")

    try:
        shutil.rmtree(str(fake_tracker_path), ignore_errors=True)
    except:
        raise