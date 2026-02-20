"""Shared pytest fixtures for all test modules"""
import data_tracker.transform_preset as tp
import data_tracker.db_manager as db
import tempfile
import shutil
import pytest
import os


@pytest.fixture
def temp_tracker_dir(monkeypatch):
    """Create a temporary .data_tracker directory with all required structure.
     - monkeypatches find_data_tracker_root() to return this temp tracker path.
     - This ensures that any calls to core will work correctly with this temp tracker
    """
    temp_dir = tempfile.mkdtemp()
    tracker_path = os.path.join(temp_dir, ".data_tracker")
    os.makedirs(os.path.join(tracker_path, "objects"))

    db_path = os.path.join(tracker_path, "tracker.db")
    success, msg = db.initialize_database(db_path)
    assert success, f"Failed to init DB: {msg}"

    tp.init_preset(tracker_path)

    monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root',
                       lambda: tracker_path)

    yield {
        'tracker_path': tracker_path,
        'db_path': db_path,
        'temp_dir': temp_dir
    }

    shutil.rmtree(temp_dir, ignore_errors=True)
