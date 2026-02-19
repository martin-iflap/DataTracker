import data_tracker.transform_preset as tp
import data_tracker.db_manager as db
import data_tracker.transform as tf
import data_tracker.core as core
from unittest.mock import patch
import tempfile
import shutil
import pytest
import os


@pytest.fixture
def temp_tracker_dir():
    """Create a temporary .data_tracker directory with all required structure"""
    temp_dir = tempfile.mkdtemp()
    tracker_path = os.path.join(temp_dir, ".data_tracker")
    os.makedirs(os.path.join(tracker_path, "objects"))

    db_path = os.path.join(tracker_path, "tracker.db")
    success, msg = db.initialize_database(db_path)
    assert success, f"Failed to init DB: {msg}"

    tp.init_preset(tracker_path)

    yield {
        'tracker_path': tracker_path,
        'db_path': db_path,
        'temp_dir': temp_dir
    }

    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
def mock_docker_transform():
    """Mock the Docker transform function to avoid actual Docker calls"""
    with patch('data_tracker.docker_manager.transform_data') as mock_transform:
        mock_transform.return_value = (True, "Transform successful")
        yield mock_transform

@pytest.fixture
def test_input_file(temp_tracker_dir):
    """Create a temporary input file for testing

    Note: Cleanup is handled by temp_tracker_dir fixture which removes
    the entire temp directory. We yield here for consistency and to make
    the lifecycle explicit.
    """
    input_path = os.path.join(temp_tracker_dir['temp_dir'], "input.csv")
    with open(input_path, 'w') as f:
        f.write("col1,col2\n1,2\n3,4\n")

    yield input_path

    try:
        if os.path.exists(input_path):
            os.remove(input_path)
    except (OSError, PermissionError):
        pass

@pytest.fixture
def test_output_dir(temp_tracker_dir):
    """Create a test output directory

    Note: Cleanup is handled by temp_tracker_dir fixture which removes
    the entire temp directory. We yield here for consistency and to make
    the lifecycle explicit.
    """
    output_path = os.path.join(temp_tracker_dir['temp_dir'], "output")
    os.makedirs(output_path, exist_ok=True)

    yield output_path

    try:
        if os.path.exists(output_path):
            shutil.rmtree(output_path, ignore_errors=True)
    except (OSError, PermissionError):
        pass

@pytest.fixture
def mock_tracker_root(temp_tracker_dir, monkeypatch):
    """Mock find_data_tracker_root to return the temp tracker path.

    This is needed for tests that use auto_track or any functionality
    that calls core.add_data/update_data, since those functions use
    find_data_tracker_root() to locate the tracker directory.
    """
    tracker_path = temp_tracker_dir['tracker_path']
    monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root',
                       lambda: tracker_path)
    return tracker_path

# ---------------------------    TESTS    ------------------------------

class TestValidateTransformEnvironment:
    """Test the validate_transform_environment function"""

    def test_validate_environment_docker_not_installed(self, monkeypatch):
        """Test validation fails when Docker is not installed"""
        monkeypatch.setattr('data_tracker.docker_manager.is_docker_installed', lambda: False)

        success, message = tf.validate_transform_environment()

        assert success is False
        assert "Docker is not installed" in message

    def test_validate_environment_tracker_not_initialized(self, monkeypatch):
        """Test validation fails when tracker not initialized"""
        monkeypatch.setattr('data_tracker.docker_manager.is_docker_installed', lambda: True)
        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root', lambda: None)

        success, message = tf.validate_transform_environment()

        assert success is False
        assert "not initialized" in message

    def test_validate_environment_success(self ,monkeypatch, temp_tracker_dir):
        """Test validation succeeds when everything is ready"""
        monkeypatch.setattr('data_tracker.docker_manager.is_docker_installed', lambda: True)
        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root',
                            lambda: temp_tracker_dir['tracker_path'])

        success, tracker_path = tf.validate_transform_environment()

        assert success is True
        assert tracker_path == temp_tracker_dir['tracker_path']



class TestExecuteTransform:
    """Test the execute transform function"""

    def test_execute_transform_with_preset_basic(self, temp_tracker_dir,
                                                 mock_docker_transform,
                                                 test_input_file, test_output_dir,
                                                 mock_tracker_root,
                                                 monkeypatch):
        """Test transform uses preset values when provided"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root',
                           lambda: tracker_path)

        success, message, metadata = tf.execute_transform(
            db_path=db_path,
            tracker_path=tracker_path,
            preset_name="example-python",
            image=None,  # Should come from preset
            input_data=test_input_file,
            output_data=test_output_dir,
            command=None,  # Should come from preset
            force=None,
            auto_track=None,
            no_track=None,
            dataset_id=None,
            message=None,
            version=None
        )

        if not success:
            print(f"\nError message: {message}")
        assert success is True
        mock_docker_transform.assert_called_once()
        call_args = mock_docker_transform.call_args[0]
        assert call_args[0] == "python:3.11-slim"
        assert "python /input/script.py" in call_args[3]

    def test_execute_transform_preset_missing_required_fields(self, temp_tracker_dir,
                                                              test_input_file, test_output_dir):
        """Test error when preset is missing required fields"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        preset_file = os.path.join(tracker_path, "presets_config.json")
        import json
        with open(preset_file, 'r') as f:
            data = json.load(f)
        data['presets']['incomplete'] = {'command': 'echo test'}  # Missing image
        with open(preset_file, 'w') as f:
            json.dump(data, f)

        success, message, metadata = tf.execute_transform(
            db_path=db_path,
            tracker_path=tracker_path,
            preset_name="incomplete",
            image=None,
            input_data=test_input_file,
            output_data=test_output_dir,
            command=None,
            force=None,
            auto_track=None,
            no_track=None,
            dataset_id=None,
            message=None,
            version=None
        )

        assert success is False
        assert "missing required fields" in message.lower()
        assert "image" in message

    def test_execute_transform_auto_track_adds_input(self, temp_tracker_dir, mock_docker_transform,
                                                     test_input_file, test_output_dir, monkeypatch):
        """Test --auto-track adds input as new dataset
         - verify dataset is not tracked initially and is tracked after transform
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root',
                            lambda: tracker_path)


        dataset_id = db.find_dataset_by_path(db_path, test_input_file)
        assert dataset_id is None

        success, message, metadata = tf.execute_transform(
            db_path=db_path,
            tracker_path=tracker_path,
            preset_name=None,
            image="python:3.11",
            input_data=test_input_file,
            output_data=test_output_dir,
            command="echo 'test'",
            force=True,
            auto_track=True,  # Should add input
            no_track=False,
            dataset_id=None,
            message=None,
            version=None
        )

        assert success is True
        assert "Added as 'dataset-1' (ID: 1)" in message

        dataset_id = db.find_dataset_by_path(db_path, test_input_file)
        assert dataset_id is not None

    def test_execute_transform_no_track_skips_versioning(self, temp_tracker_dir, mock_docker_transform,
                                                         test_input_file, test_output_dir):
        """Test --no-track skips versioning even if input is tracked
         - verify dataset is tracked but output is not tracked and message indicates versioning was skipped
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        core.add_data(test_input_file, title="test-dataset", version=1.0, message="Initial")

        success, message, metadata = tf.execute_transform(
            db_path=db_path,
            tracker_path=tracker_path,
            preset_name=None,
            image="python:3.11",
            input_data=test_input_file,
            output_data=test_output_dir,
            command="echo 'test'",
            force=True,
            auto_track=False,
            no_track=True,  # Explicit no-track
            dataset_id=None,
            message=None,
            version=None
        )

        assert success is True
        assert metadata['tracked'] is False
        assert "Versioning disabled" in message

    def test_execute_transform_tracked_input_versions_output(self, temp_tracker_dir, mock_docker_transform,
                                                             test_input_file, test_output_dir, monkeypatch):
        """Test tracked input automatically versions output
         - Add input as tracked dataset
         - Create output file to simulate transform result
         - Run transform with auto_track=False and no_track=False
         - Verify output is tracked with new version and message indicates update
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root',
                            lambda: tracker_path)

        success, msg = core.add_data(test_input_file, title="test-dataset",
                                     version=1.0, message="Initial")
        assert success, msg

        output_file = os.path.join(test_output_dir, "result.csv")
        with open(output_file, 'w') as f:
            f.write("result\n")

        success, message, metadata = tf.execute_transform(
            db_path=db_path,
            tracker_path=tracker_path,
            preset_name=None,
            image="python:3.11",
            input_data=test_input_file,
            output_data=test_output_dir,
            command="echo 'test'",
            force=True,
            auto_track=False, # Should add new version
            no_track=False,
            dataset_id=None,
            message=None,
            version=None
        )

        assert success is True
        assert metadata['tracked'] is True
        assert metadata['old_version'] == 1.0
        assert metadata['new_version'] == 1.1
        assert "Updated 'test-dataset' to version 1.1" in message

    def test_execute_transform_docker_failure_rolls_back_auto_add(self, temp_tracker_dir,
                                                                  test_input_file, test_output_dir,
                                                                  monkeypatch):
        """Test rollback when Docker fails after auto-adding input
         - Run transform with auto_track=True and mock Docker to fail
         - Verify transform fails with appropriate message and input dataset is not left in database
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root',
                           lambda: tracker_path)

        with patch('data_tracker.docker_manager.transform_data') as mock_docker:
            mock_docker.return_value = (False, "Docker container failed")

            success, message, metadata = tf.execute_transform(
                db_path=db_path,
                tracker_path=tracker_path,
                preset_name=None,
                image="python:3.11",
                input_data=test_input_file,
                output_data=test_output_dir,
                command="echo 'test'",
                force=True,
                auto_track=True,  # Will add input
                no_track=False,
                dataset_id=None,
                message=None,
                version=None
            )

        assert success is False
        assert "Transformation failed" in message
        assert "Rolled back" in message or "Removed auto-added" in message

        dataset_id = db.find_dataset_by_path(db_path, test_input_file)
        assert dataset_id is None  # Should be rolled back


