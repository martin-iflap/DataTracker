import data_tracker.status as status
import pytest


@pytest.fixture
def single_dataset(monkeypatch):
    """Fixture to set up a single dataset by monkeypatching the db functions."""
    monkeypatch.setattr(status.fu, "find_data_tracker_root", lambda: "/fake/tracker")
    monkeypatch.setattr(status.db, "get_all_datasets", lambda db_path: [{
        'id' : 1, 'name': 'Test-dataset'
    }])

    monkeypatch.setattr(status.db, "get_latest_version_info", lambda db_path, dataset_id: {
        'version': '1.0',
        'original_path': '/fake/path/to/dataset.csv',
        'object_hash': 'fakehash'
    })

# --------------------------   Tests   --------------------------

def test_status_no_tracker(monkeypatch):
    """Test the status command when the data tracker is not initialized."""
    monkeypatch.setattr(status.fu, "find_data_tracker_root", lambda: None)
    success, message = status.get_status(detailed=False)
    assert not success
    assert "Data tracker is not initialized" in message

def test_status_empty_tracker(monkeypatch):
    """Test the status command when the data tracker is has no datasets"""
    monkeypatch.setattr(status.fu, "find_data_tracker_root", lambda: "/fake/tracker")
    monkeypatch.setattr(status.db, "get_all_datasets", lambda db_path: [])
    success, message = status.get_status(detailed=False)
    assert success
    assert "Data tracker is initialized but no datasets are being tracked" in message

def test_status_no_versions_no_detailed(single_dataset, monkeypatch):
    """Test the status command when a dataset is tracked but has no versions."""
    monkeypatch.setattr(status.db, "get_latest_version_info", lambda db_path, dataset_id: None)
    success, message = status.get_status(detailed=False)
    assert success
    assert "no versions found" in message

def test_status_no_versions_detailed(single_dataset, monkeypatch):
    """Test the status command when a dataset is tracked but has no versions, with detailed=True."""
    monkeypatch.setattr(status.db, "get_dataset_history", lambda db_path, dataset_id, name: [])
    success, message = status.get_status(detailed=True)
    print(message)
    assert success
    assert "no versions found" in message

def test_status_single_dataset_missing_file(single_dataset, monkeypatch):
    """Test the status command with a single dataset that has version up to date"""
    monkeypatch.setattr(status.os.path, "exists", lambda path: False)
    success, message = status.get_status(detailed=False)
    assert success
    assert "Not found at" in message

def test_status_single_dataset_up_to_date(single_dataset, monkeypatch):
    """Test the status command with a single dataset that has version up to date"""
    monkeypatch.setattr(status.os.path, "exists", lambda path: True)
    monkeypatch.setattr(status.os.path, "isfile", lambda path: True)
    monkeypatch.setattr(status.fu, "hash_file", lambda path: "fakehash")
    success, message = status.get_status(detailed=False)
    assert success
    assert "Up to date" in message

def test_status_single_dataset_modified_no_detail(single_dataset, monkeypatch):
    """Test the status command with a single dataset that has been modified without detailed flag"""
    monkeypatch.setattr(status.os.path, "exists", lambda path: True)
    monkeypatch.setattr(status.os.path, "isfile", lambda path: True)
    monkeypatch.setattr(status.fu, "hash_file", lambda path: "different_hash")
    success, message = status.get_status(detailed=False)
    assert success
    assert "Modified" in message
    assert "matches" not in message
    assert "no matching version" not in message

def test_status_single_dataset_modified_with_detail(single_dataset, monkeypatch):
    """Test the status command with a single dataset that has been modified with detailed flag"""
    monkeypatch.setattr(status.os.path, "exists", lambda path: True)
    monkeypatch.setattr(status.os.path, "isfile", lambda path: True)
    monkeypatch.setattr(status.fu, "hash_file", lambda path: "different_hash")
    monkeypatch.setattr(status.db, "get_dataset_history", lambda db_path, dataset_id, name: [
        {'version': '0.9', 'object_hash': 'different_hash', 'original_path': '/fake/path/to/file.csv'},
        {'version': '1.0', 'object_hash': 'fakehash', 'original_path': '/fake/path/to/dataset.csv'}
    ])
    success, message = status.get_status(detailed=True)
    assert success
    assert "Modified (matches v0.9)" in message

def test_status_single_dataset_modified_no_matching_version(single_dataset, monkeypatch):
    """Test the status command with single dataset and modified with detailed flag but no matching version in history"""
    monkeypatch.setattr(status.os.path, "exists", lambda path: True)
    monkeypatch.setattr(status.os.path, "isfile", lambda path: True)
    monkeypatch.setattr(status.fu, "hash_file", lambda path: "different_hash")
    monkeypatch.setattr(status.db, "get_dataset_history", lambda db_path, dataset_id, name: [
        {'version': '0.9', 'object_hash': 'some_other_hash', 'original_path': '/fake/path/to/file.csv'},
        {'version': '1.0', 'object_hash': 'fakehash', 'original_path': '/fake/path/to/dataset.csv'}
    ])
    success, message = status.get_status(detailed=True)
    assert success
    assert "Modified (no version matching the current state found)" in message