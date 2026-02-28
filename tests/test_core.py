from unittest.mock import Mock, MagicMock
from data_tracker import core
import tempfile
import sqlite3
import pytest
import os


@pytest.fixture
def temp_file():
    """Create a temporary file that auto-deletes after test."""
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.close()
    yield temp.name
    try:
        os.unlink(temp.name)
    except (OSError, FileNotFoundError):
        pass


@pytest.fixture
def temp_tracker_path(temp_dir):
    """Create a temporary .data_tracker structure without database initialization."""
    tracker_path = os.path.join(temp_dir, ".data_tracker")
    os.makedirs(os.path.join(tracker_path, "objects"))
    yield tracker_path


@pytest.fixture
def mock_db_connection():
    """Create a reusable mock database connection for core db operations."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=False)
    return mock_conn


def _setup_db_for_new_dataset(monkeypatch, mock_conn, *, hash_exists=None):
    """Common db mocks for creating a new dataset in _add_files_to_tracker.
    - open_database returns mock_conn
    - dataset_exists is False
    - insert_dataset returns a fixed id
    - hash_exists can be customized to simulate hash collisions
    - insert_version / insert_object / insert_files are mocked for assertions
    """
    monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_conn))
    monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=False))
    monkeypatch.setattr('data_tracker.core.db.insert_dataset', Mock(return_value=123))
    monkeypatch.setattr('data_tracker.core.db.hash_exists', Mock(return_value=hash_exists))
    monkeypatch.setattr('data_tracker.core.db.insert_version', Mock(return_value=456))
    monkeypatch.setattr('data_tracker.core.db.check_version_exists', Mock(return_value=False))
    monkeypatch.setattr('data_tracker.core.db.insert_object', Mock())
    monkeypatch.setattr('data_tracker.core.db.insert_files', Mock())


def _setup_file_ops(monkeypatch, *, hash_file_values=None, dir_hash=None, size=1000):
    """Common file utility mocks for _add_files_to_tracker.
    - hash_file can return a single value or a sequence via side_effect
    - hash_directory can be set when testing directory primary hashes
    - getsize is fixed to a known value
    """
    if hash_file_values is not None:
        if isinstance(hash_file_values, list):
            monkeypatch.setattr('data_tracker.core.fu.hash_file', Mock(side_effect=hash_file_values))
        else:
            monkeypatch.setattr('data_tracker.core.fu.hash_file', Mock(return_value=hash_file_values))
    if dir_hash is not None:
        monkeypatch.setattr('data_tracker.core.fu.hash_directory', Mock(return_value=dir_hash))
    monkeypatch.setattr('data_tracker.core.fu.copy_file_to_objects', Mock())
    monkeypatch.setattr('os.path.getsize', Mock(return_value=size))

# ==================== TESTS: initialize_tracker ====================

class TestInitializeTracker:
    """Test initialization of the .data_tracker directory"""

    def test_initialize_success(self, temp_dir, monkeypatch):
        """Test successful tracker initialization"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)
        monkeypatch.setattr('os.getcwd', lambda: temp_dir)

        mock_init_preset = Mock()
        monkeypatch.setattr('data_tracker.core.tp.init_preset', mock_init_preset)

        mock_init_db = Mock(return_value=(True, "Database initialized"))
        monkeypatch.setattr('data_tracker.core.db.initialize_database', mock_init_db)

        success, msg = core.initialize_tracker()

        assert success
        assert "Database initialized" in msg

        # Verify directory structure was created
        tracker_path = os.path.join(temp_dir, ".data_tracker")
        assert os.path.exists(tracker_path)
        assert os.path.exists(os.path.join(tracker_path, "objects"))

        # Verify init_preset was called
        mock_init_preset.assert_called_once_with(tracker_path)

        # Verify db.initialize_database was called
        mock_init_db.assert_called_once()

    def test_initialize_already_initialized(self, monkeypatch):
        """Test that initialization fails if tracker already exists"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/existing/tracker")

        success, msg = core.initialize_tracker()

        assert not success
        assert "already initialized" in msg
        assert "/existing/tracker" in msg

    def test_initialize_makedirs_failure(self, monkeypatch):
        """Test handling of OSError during directory creation"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)
        monkeypatch.setattr('os.getcwd', lambda: "/some/path")

        def mock_makedirs(*args, **kwargs):
            raise OSError("Permission denied")

        monkeypatch.setattr('os.makedirs', mock_makedirs)

        success, msg = core.initialize_tracker()

        assert not success
        assert "Failed to create essential directories" in msg
        assert "Permission denied" in msg

    def test_initialize_database_error(self, temp_dir, monkeypatch):
        """Test handling of sqlite3.Error during database initialization"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)
        monkeypatch.setattr('os.getcwd', lambda: temp_dir)
        monkeypatch.setattr('data_tracker.core.tp.init_preset', Mock())

        def mock_init_db(db_path):
            raise sqlite3.Error("Database locked")

        monkeypatch.setattr('data_tracker.core.db.initialize_database', mock_init_db)

        success, msg = core.initialize_tracker()

        assert not success
        assert "Database initialization error" in msg
        assert "Database locked" in msg

# ==================== TESTS: validate_dataset_name ====================

class TestDatasetNameValidation:
    """Test dataset name validation logic"""

    def test_valid_simple_name(self):
        """Test a simple valid dataset name"""
        is_valid, result = core.validate_dataset_name("my-dataset")
        assert is_valid is True
        assert result == "my-dataset"

    def test_valid_name_with_spaces(self):
        """Test dataset name with spaces"""
        is_valid, result = core.validate_dataset_name("My Dataset 2024")
        assert is_valid is True
        assert result == "My Dataset 2024"

    def test_valid_name_with_unicode(self):
        """Test dataset name with Unicode characters"""
        is_valid, result = core.validate_dataset_name("データセット")
        assert is_valid is True
        assert result == "データセット"

    def test_strips_whitespace(self):
        """Test that leading/trailing whitespace is stripped"""
        is_valid, result = core.validate_dataset_name("  dataset  ")
        assert is_valid is True
        assert result == "dataset"

    def test_none_is_valid(self):
        """Test that None is valid (auto-generates name)"""
        is_valid, result = core.validate_dataset_name(None)
        assert is_valid is True
        assert result is None

    def test_empty_string_invalid(self):
        """Test that empty string is invalid"""
        is_valid, result = core.validate_dataset_name("")
        assert is_valid is False
        assert "empty" in result.lower()

    def test_only_whitespace_invalid(self):
        """Test that only whitespace is invalid"""
        is_valid, result = core.validate_dataset_name("   ")
        assert is_valid is False
        assert "empty" in result.lower()

    def test_too_long_name(self):
        """Test that names over 100 characters are rejected"""
        long_name = "a" * 101
        is_valid, result = core.validate_dataset_name(long_name)
        assert is_valid is False
        assert "too long" in result.lower()
        assert "101" in result

    def test_exactly_100_chars_valid(self):
        """Test that exactly 100 characters is valid"""
        name = "a" * 100
        is_valid, result = core.validate_dataset_name(name)
        assert is_valid is True
        assert result == name

    def test_newline_invalid(self):
        """Test that newlines are rejected"""
        is_valid, result = core.validate_dataset_name("dataset\nname")
        assert is_valid is False
        assert "control characters" in result.lower()

    def test_all_digits_valid(self):
        """Test that all-digit names are valid but could be confusing"""
        is_valid, result = core.validate_dataset_name("12345")
        assert is_valid is True
        assert result == "12345"

    def test_special_chars_valid(self):
        """Test that common special characters are allowed"""
        is_valid, result = core.validate_dataset_name("dataset_v1.0-final(2)")
        assert is_valid is True
        assert result == "dataset_v1.0-final(2)"

# ==================== TESTS: add_data ====================

class TestAddData:
    """Test adding new data to tracker"""

    def test_add_data_single_file(self, temp_file, monkeypatch):
        """Test adding a single file"""
        with open(temp_file, "wb") as f:
            f.write(b"test content")

        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        mock_add_files = Mock(return_value=(True, "File added successfully"))
        monkeypatch.setattr('data_tracker.core._add_files_to_tracker', mock_add_files)

        success, msg = core.add_data(temp_file, "test-dataset", 1.0, "test message")

        assert success
        assert "File added successfully" in msg

        mock_add_files.assert_called_once()
        call_args = mock_add_files.call_args
        files_arg = call_args[0][0]
        assert len(files_arg) == 1
        assert files_arg[0][0] == os.path.abspath(temp_file)
        assert files_arg[0][1] == os.path.basename(temp_file)

    def test_add_data_directory(self, temp_dir, monkeypatch):
        """Test adding a directory with multiple files"""
        # Create test structure
        os.makedirs(os.path.join(temp_dir, "subdir"))
        with open(os.path.join(temp_dir, "file1.txt"), "w") as f:
            f.write("content1")
        with open(os.path.join(temp_dir, "subdir", "file2.txt"), "w") as f:
            f.write("content2")

        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        mock_add_files = Mock(return_value=(True, "Files added successfully"))
        monkeypatch.setattr('data_tracker.core._add_files_to_tracker', mock_add_files)

        success, msg = core.add_data(temp_dir, "test-dataset", 1.0, "test message")

        assert success

        # Verify _add_files_to_tracker was called with multiple files
        call_args = mock_add_files.call_args
        files_arg = call_args[0][0]
        assert len(files_arg) == 2

        # Check relative paths are correct
        rel_paths = [f[1] for f in files_arg]
        assert "file1.txt" in rel_paths
        assert os.path.join("subdir", "file2.txt") in rel_paths

    def test_add_data_invalid_title(self, temp_file, monkeypatch):
        """Test that invalid dataset name is rejected"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        success, msg = core.add_data(temp_file, "", 1.0, "message")

        assert not success
        assert "Invalid dataset name" in msg
        assert "empty" in msg.lower()

    def test_add_data_tracker_not_initialized(self, monkeypatch):
        """Test error when tracker is not initialized"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)

        success, msg = core.add_data("/some/path", "dataset", 1.0, "message")

        assert not success
        assert "not initialized" in msg

    def test_add_data_path_does_not_exist(self, monkeypatch):
        """Test error when data path doesn't exist"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        success, msg = core.add_data("/nonexistent/path", "dataset", 1.0, "message")

        assert not success
        assert "does not exist" in msg

    def test_add_data_propagates_add_files_error(self, temp_file, monkeypatch):
        """Test that errors from _add_files_to_tracker are propagated"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        mock_add_files = Mock(return_value=(False, "Database error"))
        monkeypatch.setattr('data_tracker.core._add_files_to_tracker', mock_add_files)

        success, msg = core.add_data(temp_file, "dataset", 1.0, "message")

        assert not success
        assert "Database error" in msg

    def test_add_data_os_walk_failure(self, temp_dir, monkeypatch):
        """Test handling of OSError during os.walk when adding a directory"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        def mock_os_walk(*args, **kwargs):
            raise OSError("Permission denied")

        monkeypatch.setattr('os.walk', mock_os_walk)

        success, msg = core.add_data(temp_dir, "dataset", 1.0, "message")

        assert not success
        assert "File operation failed:" in msg
        assert "Permission denied" in msg

# ==================== TESTS: _add_files_to_tracker ====================

class TestAddFilesToTracker:
    """Test the core file addition logic"""

    def test_add_new_dataset_single_file(self, temp_tracker_path, mock_db_connection, monkeypatch):
        """Test adding a new dataset with a single file."""
        _setup_db_for_new_dataset(monkeypatch, mock_db_connection, hash_exists=None)
        _setup_file_ops(monkeypatch, hash_file_values="hash123")

        files = [("/path/to/file.txt", "file.txt")]

        success, msg = core._add_files_to_tracker(
            files, temp_tracker_path, "/path/to/file.txt",
            title="test-dataset", version=1.0, message="test message"
        )

        assert success
        assert "added successfully" in msg

        core.db.insert_dataset.assert_called_once_with(mock_db_connection, "test-dataset", "test message")
        core.db.insert_version.assert_called_once()
        core.db.insert_files.assert_called_once_with(mock_db_connection, 456, "hash123", "file.txt")

    def test_add_multiple_files_to_new_dataset(self, temp_tracker_path, mock_db_connection, monkeypatch):
        """Test adding multiple files to a new dataset and using directory hash as primary."""
        _setup_db_for_new_dataset(monkeypatch, mock_db_connection, hash_exists=None)
        _setup_file_ops(monkeypatch, hash_file_values=["hash1", "hash2"], dir_hash="dirhash")

        files = [
            ("/path/to/file1.txt", "file1.txt"),
            ("/path/to/file2.txt", "file2.txt"),
        ]

        success, msg = core._add_files_to_tracker(
            files, temp_tracker_path, "/path/to",
            title="test-dataset", version=1.0, message="test message"
        )

        assert success
        core.fu.hash_directory.assert_called_once_with("/path/to")
        assert core.db.insert_files.call_count == 2

    def test_update_existing_dataset_auto_version(self, temp_tracker_path, mock_db_connection, monkeypatch):
        """Test updating existing dataset with auto-incremented version."""
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.get_latest_version', Mock(return_value=2.0))
        monkeypatch.setattr('data_tracker.core.db.hash_exists', Mock(return_value=None))
        monkeypatch.setattr('data_tracker.core.db.insert_version', Mock(return_value=789))
        monkeypatch.setattr('data_tracker.core.db.insert_object', Mock())
        monkeypatch.setattr('data_tracker.core.db.insert_files', Mock())

        _setup_file_ops(monkeypatch, hash_file_values="hash123")

        files = [("/path/to/file.txt", "file.txt")]

        success, msg = core._add_files_to_tracker(
            files, temp_tracker_path, "/path/to/file.txt",
            dataset_id=123, version=None, message="update message"
        )

        assert success
        core.db.get_latest_version.assert_called_once_with(mock_db_connection, 123)
        core.db.insert_version.assert_called_once()
        version_call_args = core.db.insert_version.call_args[0]
        assert version_call_args[3] == 3.0

    def test_version_already_exists_error(self, mock_db_connection, monkeypatch):
        """Test error when version already exists for a dataset."""
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.check_version_exists', Mock(return_value=True))

        success, msg = core._add_files_to_tracker(
            [("/file", "file")], "/tracker", "/file",
            dataset_id=123, version=1.0
        )

        assert not success
        assert "already exists" in msg

    def test_duplicate_title_error(self, mock_db_connection, monkeypatch):
        """Test error when creating dataset with an existing title."""
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=True))

        success, msg = core._add_files_to_tracker(
            [("/file", "file")], "/tracker", "/file",
            title="existing-dataset"
        )

        assert not success
        assert "already exists" in msg

    def test_hash_collision_warning(self, temp_tracker_path, mock_db_connection, monkeypatch):
        """Test that hash collision returns a duplicate warning message."""
        _setup_db_for_new_dataset(
            monkeypatch,
            mock_db_connection,
            hash_exists="Version: 1.0, Path: /old/path",
        )
        _setup_file_ops(monkeypatch, hash_file_values="hash123")

        success, msg = core._add_files_to_tracker(
            [("/file", "file")], temp_tracker_path, "/file",
            title="test"
        )

        assert success
        assert "Duplicate Warning!" in msg
        assert "Version: 1.0" in msg

    def test_sqlite_error_with_object_cleanup(self, temp_tracker_path, mock_db_connection, monkeypatch):
        """Test that unused objects are cleaned up when insert_files raises sqlite3.Error."""
        _setup_db_for_new_dataset(monkeypatch, mock_db_connection, hash_exists=None)
        monkeypatch.setattr(
            'data_tracker.core.db.insert_files',
            Mock(side_effect=sqlite3.Error("Constraint violation")),
        )
        monkeypatch.setattr('data_tracker.core.db.object_is_used', Mock(return_value=False))

        _setup_file_ops(monkeypatch, hash_file_values="hash123")

        mock_remove = Mock(return_value=(True, ""))
        monkeypatch.setattr('data_tracker.core._remove_file_object', mock_remove)

        success, msg = core._add_files_to_tracker(
            [("/file", "file")], temp_tracker_path, "/file",
            title="test"
        )

        assert not success
        assert "Database error" in msg
        mock_remove.assert_called_once_with(temp_tracker_path, "hash123")

    def test_sqlite_error_object_in_use_no_cleanup(self, temp_tracker_path, mock_db_connection, monkeypatch):
        """Test that objects still in use are not cleaned up on sqlite3.Error."""
        _setup_db_for_new_dataset(monkeypatch, mock_db_connection, hash_exists=None)
        monkeypatch.setattr(
            'data_tracker.core.db.insert_files',
            Mock(side_effect=sqlite3.Error("Error")),
        )
        monkeypatch.setattr('data_tracker.core.db.object_is_used', Mock(return_value=True))

        _setup_file_ops(monkeypatch, hash_file_values="hash123")

        mock_remove = Mock()
        monkeypatch.setattr('data_tracker.core._remove_file_object', mock_remove)

        success, msg = core._add_files_to_tracker(
            [("/file", "file")], temp_tracker_path, "/file",
            title="test"
        )

        assert not success

        mock_remove.assert_not_called()

# ==================== TESTS: list_data ====================

class TestListData:
    """Test listing tracked datasets"""

    def test_list_tracker_not_initialized(self, monkeypatch):
        """Test error when tracker is not initialized"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)

        success, msg = core.list_data(struct=False)

        assert not success
        assert "not initialized" in msg

    def test_list_no_datasets(self, monkeypatch):
        """Test message when no datasets exist"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.get_all_datasets', Mock(return_value=[]))

        success, msg = core.list_data(struct=False)

        assert success
        assert "No datasets tracked yet" in msg

    def test_list_datasets_without_structure(self, monkeypatch):
        """Test listing datasets without structure display"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        mock_datasets = [
            {'id': 1, 'name': 'dataset1', 'created_at': '2024-01-01', 'message': 'msg1'},
            {'id': 2, 'name': 'dataset2', 'created_at': '2024-01-02', 'message': 'msg2'}
        ]
        monkeypatch.setattr('data_tracker.core.db.get_all_datasets', Mock(return_value=mock_datasets))

        success, msg = core.list_data(struct=False)

        assert success
        assert "Tracked Datasets:" in msg
        assert "ID: 1" in msg
        assert "dataset1" in msg
        assert "ID: 2" in msg
        assert "dataset2" in msg

    def test_list_datasets_with_structure(self, monkeypatch):
        """Test listing datasets with structure display"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        mock_datasets = [
            {'id': 1, 'name': 'dataset1', 'created_at': '2024-01-01', 'message': 'msg1'}
        ]
        monkeypatch.setattr('data_tracker.core.db.get_all_datasets', Mock(return_value=mock_datasets))
        monkeypatch.setattr('data_tracker.core.fu.display_structure',
                          Mock(return_value="  Structure:\n    -file.txt"))

        success, msg = core.list_data(struct=True)

        assert success
        assert "Structure:" in msg
        assert "-file.txt" in msg

# ==================== TESTS: get_history ====================

class TestGetHistory:
    """Test retrieving dataset history"""

    def test_history_tracker_not_initialized(self, monkeypatch):
        """Test error when tracker is not initialized"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)

        success, msg = core.get_history(1, None, False)

        assert not success
        assert "not initialized" in msg

    def test_history_no_history_found(self, monkeypatch):
        """Test message when no history exists"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.get_dataset_history', Mock(return_value=[]))

        success, msg = core.get_history(999, None, False)

        assert success
        assert "No history found" in msg

    def test_history_non_detailed(self, monkeypatch):
        """Test non-detailed history display"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        mock_history = [
            {'id': 1, 'version': 1.0, 'message': 'First version', 'created_at': '2024-01-01',
             'original_path': '/path', 'object_hash': 'hash1'},
            {'id': 2, 'version': 2.0, 'message': 'Second version', 'created_at': '2024-01-02',
             'original_path': '/path', 'object_hash': 'hash2'}
        ]
        monkeypatch.setattr('data_tracker.core.db.get_dataset_history', Mock(return_value=mock_history))

        success, msg = core.get_history(1, None, detailed_flag=False)

        assert success
        assert "Dataset History:" in msg
        assert "Version: 1.0" in msg
        assert "First version" in msg
        assert "Version: 2.0" in msg
        assert "Second version" in msg
        # Should NOT include detailed fields
        assert "Object Hash:" not in msg
        assert "Original Path:" not in msg

    def test_history_detailed(self, monkeypatch):
        """Test detailed history display"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        mock_history = [
            {'id': 1, 'version': 1.0, 'message': 'First version', 'created_at': '2024-01-01',
             'original_path': '/path/to/data', 'object_hash': 'abc123def456'}
        ]
        monkeypatch.setattr('data_tracker.core.db.get_dataset_history', Mock(return_value=mock_history))

        success, msg = core.get_history(1, None, detailed_flag=True)

        assert success
        assert "Dataset History:" in msg
        assert "Version: 1.0" in msg
        assert "ID: 1" in msg
        assert "First version" in msg
        assert "Original Path: /path/to/data" in msg
        assert "Object Hash: abc123def456" in msg

# ==================== TESTS: update_data ====================

class TestUpdateData:
    """Test updating existing datasets"""

    def test_update_tracker_not_initialized(self, monkeypatch):
        """Test error when tracker is not initialized"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)

        success, msg = core.update_data("/path", 1, None, 2.0, "message")

        assert not success
        assert "not initialized" in msg

    def test_update_path_does_not_exist(self, monkeypatch):
        """Test error when data path doesn't exist"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")

        success, msg = core.update_data("/nonexistent", 1, None, 2.0, "message")

        assert not success
        assert "does not exist" in msg

    def test_update_dataset_does_not_exist(self, temp_file, mock_db_connection, monkeypatch):
        """Test error when dataset doesn't exist"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=False))

        success, msg = core.update_data(temp_file, 999, None, 2.0, "message")

        assert not success
        assert "does not exist" in msg

    def test_update_with_name_resolves_id(self, temp_file, mock_db_connection, monkeypatch):
        """Test that dataset name is resolved to ID"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=True))
        monkeypatch.setattr('data_tracker.core.db.get_id_from_name', Mock(return_value=123))

        mock_add_files = Mock(return_value=(True, "Updated"))
        monkeypatch.setattr('data_tracker.core._add_files_to_tracker', mock_add_files)

        success, msg = core.update_data(temp_file, None, "test-dataset", 2.0, "message")

        assert success

        core.db.get_id_from_name.assert_called_once_with(mock_db_connection, "test-dataset")

        call_kwargs = mock_add_files.call_args[1]
        assert call_kwargs['dataset_id'] == 123

    def test_update_single_file(self, temp_file, mock_db_connection, monkeypatch):
        """Test updating with a single file"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=True))

        mock_add_files = Mock(return_value=(True, "Updated"))
        monkeypatch.setattr('data_tracker.core._add_files_to_tracker', mock_add_files)

        success, msg = core.update_data(temp_file, 123, None, 2.0, "message")

        assert success

        # Verify files list has single entry
        call_args = mock_add_files.call_args
        files_arg = call_args[0][0]
        assert len(files_arg) == 1

# ==================== TESTS: remove_data ====================

class TestRemoveData:
    """Test removing datasets"""

    def test_remove_tracker_not_initialized(self, monkeypatch):
        """Test error when tracker is not initialized"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: None)

        success, msg = core.remove_data(1, None)

        assert not success
        assert "not initialized" in msg

    def test_remove_dataset_does_not_exist(self, mock_db_connection, monkeypatch):
        """Test error when dataset doesn't exist"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=False))

        success, msg = core.remove_data(999, None)

        assert not success
        assert "does not exist" in msg

    def test_remove_with_name_resolves_id(self, mock_db_connection, monkeypatch):
        """Test that dataset name is resolved to ID"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=True))
        monkeypatch.setattr('data_tracker.core.db.get_id_from_name', Mock(return_value=123))
        monkeypatch.setattr('data_tracker.core.db.delete_files', Mock())
        monkeypatch.setattr('data_tracker.core.db.delete_versions', Mock())
        monkeypatch.setattr('data_tracker.core.db.delete_object', Mock(return_value=[]))
        monkeypatch.setattr('data_tracker.core.db.delete_dataset', Mock())

        success, msg = core.remove_data(None, "test-dataset")

        assert success

        core.db.get_id_from_name.assert_called_once_with(mock_db_connection, "test-dataset")

    def test_remove_success_with_object_cleanup(self, mock_db_connection, monkeypatch):
        """Test successful removal with object file cleanup"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=True))
        monkeypatch.setattr('data_tracker.core.db.delete_files', Mock())
        monkeypatch.setattr('data_tracker.core.db.delete_versions', Mock())
        monkeypatch.setattr('data_tracker.core.db.delete_object', Mock(return_value=["hash1", "hash2"]))
        monkeypatch.setattr('data_tracker.core.db.delete_dataset', Mock())

        mock_remove_object = Mock(return_value=(True, ""))
        monkeypatch.setattr('data_tracker.core._remove_file_object', mock_remove_object)

        success, msg = core.remove_data(123, None)

        assert success
        assert "removed successfully" in msg

        # Verify all delete operations were called
        core.db.delete_files.assert_called_once_with(mock_db_connection, 123)
        core.db.delete_versions.assert_called_once_with(mock_db_connection, 123)
        core.db.delete_dataset.assert_called_once_with(mock_db_connection, 123)

        # Verify object files were removed
        assert mock_remove_object.call_count == 2
        mock_remove_object.assert_any_call("/tracker", "hash1")
        mock_remove_object.assert_any_call("/tracker", "hash2")

    def test_remove_object_cleanup_failure(self, mock_db_connection, monkeypatch):
        """Test error when object file cleanup fails"""
        monkeypatch.setattr('data_tracker.core.fu.find_data_tracker_root', lambda: "/tracker")
        monkeypatch.setattr('data_tracker.core.db.open_database', Mock(return_value=mock_db_connection))
        monkeypatch.setattr('data_tracker.core.db.dataset_exists', Mock(return_value=True))
        monkeypatch.setattr('data_tracker.core.db.delete_files', Mock())
        monkeypatch.setattr('data_tracker.core.db.delete_versions', Mock())
        monkeypatch.setattr('data_tracker.core.db.delete_object', Mock(return_value=["hash1"]))
        monkeypatch.setattr('data_tracker.core.db.delete_dataset', Mock())

        mock_remove_object = Mock(return_value=(False, "Permission denied"))
        monkeypatch.setattr('data_tracker.core._remove_file_object', mock_remove_object)

        success, msg = core.remove_data(123, None)

        assert not success
        assert "Failed to remove object file" in msg
        assert "Permission denied" in msg

# ==================== TESTS: _remove_file_object ====================

class TestRemoveFileObject:
    """Test removing object files from filesystem"""

    def test_remove_file_success(self, temp_dir):
        """Test successfully removing an object file"""
        tracker_path = os.path.join(temp_dir, ".data_tracker")
        objects_dir = os.path.join(tracker_path, "objects")
        os.makedirs(objects_dir)

        test_hash = "test_hash_123"
        object_file = os.path.join(objects_dir, test_hash)
        with open(object_file, "w") as f:
            f.write("test content")

        success, msg = core._remove_file_object(tracker_path, test_hash)

        assert success
        assert msg == ""
        assert not os.path.exists(object_file)

    def test_remove_file_not_found_ignored(self, temp_dir):
        """Test that FileNotFoundError is ignored"""
        tracker_path = os.path.join(temp_dir, ".data_tracker")
        os.makedirs(tracker_path)

        success, msg = core._remove_file_object(tracker_path, "nonexistent_hash")

        assert success
        assert msg == ""

    def test_remove_file_oserror(self, monkeypatch):
        """Test handling of OSError during file removal"""
        def mock_remove(path):
            raise OSError("Permission denied")

        monkeypatch.setattr('os.remove', mock_remove)

        success, msg = core._remove_file_object("/tracker", "hash123")

        assert not success
        assert "Failed to remove object file" in msg
        assert "Permission denied" in msg
