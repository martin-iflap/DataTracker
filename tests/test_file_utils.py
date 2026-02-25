from data_tracker import file_utils as fu
from data_tracker import db_manager as db
import tempfile
import hashlib
import shutil
import pytest
import sys
import os


# ==================== FIXTURES ====================

@pytest.fixture
def temp_file_structure():
    """Fixture to create a temporary file structure for testing."""
    temp_dir = tempfile.mkdtemp()
    try:
        test_file1 = os.path.join(temp_dir, "dt_view_test1.csv")
        test_file2 = os.path.join(temp_dir, "dt_view_test2.html")
        other_file = os.path.join(temp_dir, "other_file.txt")

        for filepath in [test_file1, test_file2, other_file]:
            with open(filepath, "w") as f:
                f.write("test content")

        yield {"temp_dir": temp_dir, "test_file1": test_file1, "test_file2": test_file2, "other_file": other_file}
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_files_for_hashing():
    """Create temporary files and directories with known content for hash testing.
     - Create a single file with known content and calculate the hash manually for testing
     - Create a directory structure with multiple files to test directory hashing consistency
     - This fixture provides both a file and a directory to test both hash_file and hash_directory
    """
    temp_dir = tempfile.mkdtemp()
    try:
        test_file = os.path.join(temp_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("known content for hashing")

        expected_hash = hashlib.sha256(b"known content for hashing").hexdigest()

        test_dir = os.path.join(temp_dir, "test_directory")
        os.makedirs(os.path.join(test_dir, "subdir"))

        with open(os.path.join(test_dir, "file1.txt"), "w") as f:
            f.write("content1")
        with open(os.path.join(test_dir, "file2.txt"), "w") as f:
            f.write("content2")
        with open(os.path.join(test_dir, "subdir", "file3.txt"), "w") as f:
            f.write("content3")

        yield {
            "temp_dir": temp_dir,
            "test_file": test_file,
            "expected_hash": expected_hash,
            "test_dir": test_dir
        }
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def nested_tracker_structure():
    """Create a nested directory structure to test find_data_tracker_root upward traversal.
     - Creates a .data_tracker directory at the root of the temp directory
     - Creates nested subdirectories several levels deep
    """
    temp_dir = tempfile.mkdtemp()
    try:
        tracker_dir = os.path.join(temp_dir, ".data_tracker")
        os.makedirs(tracker_dir)

        nested_path = os.path.join(temp_dir, "level1", "level2", "level3")
        os.makedirs(nested_path)

        yield {
            "temp_dir": temp_dir,
            "tracker_dir": tracker_dir,
            "nested_path": nested_path
        }
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

# ==================== TESTS: format_size ====================

class TestFormatSize:
    """Test cases for the format_size function"""

    def test_format_size(self):
        """Test the format_size function formats sizes correctly."""
        assert fu.format_size(0) == "0.00 B"
        assert fu.format_size(500) == "500.00 B"
        assert fu.format_size(1024) == "1.00 KB"
        assert fu.format_size(1536) == "1.50 KB"
        assert fu.format_size(1048576) == "1.00 MB"
        assert fu.format_size(1073741824) == "1.00 GB"
        assert fu.format_size(1099511627776) == "1.00 TB"

# ==================== TESTS: find_data_tracker_root ====================

class TestFindDataTrackerRoot:
    """Test cases for the find_data_tracker_root function"""

    def test_find_in_current_directory(self, nested_tracker_structure):
        """Test finding .data_tracker in the current directory"""
        result = fu.find_data_tracker_root(nested_tracker_structure["temp_dir"])
        assert result == nested_tracker_structure["tracker_dir"]

    def test_find_in_parent_directories(self, nested_tracker_structure):
        """Test finding .data_tracker by traversing upward through parent directories"""
        result = fu.find_data_tracker_root(nested_tracker_structure["nested_path"])
        assert result == nested_tracker_structure["tracker_dir"]

    def test_not_found_returns_none(self):
        """Test that None is returned when .data_tracker is not found"""
        temp_dir = tempfile.mkdtemp()
        try:
            result = fu.find_data_tracker_root(temp_dir)
            assert result is None
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_uses_current_directory_when_no_start_path(self, nested_tracker_structure, monkeypatch):
        """Test that find_data_tracker_root uses os.getcwd() when no start_path is provided"""
        monkeypatch.setattr(os, 'getcwd', lambda: nested_tracker_structure["nested_path"])
        result = fu.find_data_tracker_root()
        assert result == nested_tracker_structure["tracker_dir"]

    def test_handles_oserror_gracefully(self, monkeypatch):
        """Test that OSError is handled and None is returned"""
        def mock_abspath(path):
            raise OSError("Mocked OSError")

        monkeypatch.setattr(os.path, 'abspath', mock_abspath)
        result = fu.find_data_tracker_root("/some/path")
        assert result is None

# ==================== TESTS: hash_file ====================

class TestHashFile:
    """Test cases for the hash_file function"""

    def test_hash_file_with_known_content(self, temp_files_for_hashing):
        """Test hashing a file with known content produces expected hash"""
        result = fu.hash_file(temp_files_for_hashing["test_file"])
        assert result == temp_files_for_hashing["expected_hash"]

    def test_hash_file_consistency(self, temp_files_for_hashing):
        """Test that hashing the same file multiple times produces the same hash"""
        hash1 = fu.hash_file(temp_files_for_hashing["test_file"])
        hash2 = fu.hash_file(temp_files_for_hashing["test_file"])
        assert hash1 == hash2

    def test_hash_file_raises_for_directory(self, temp_files_for_hashing):
        """Test that ValueError is raised when trying to hash a directory"""
        with pytest.raises(ValueError, match="is not a valid file"):
            fu.hash_file(temp_files_for_hashing["test_dir"])

    def test_hash_file_raises_for_nonexistent(self):
        """Test that ValueError is raised for non-existent file"""
        with pytest.raises(ValueError, match="is not a valid file"):
            fu.hash_file("/nonexistent/path/file.txt")

# ==================== TESTS: hash_directory ====================

class TestHashDirectory:
    """Test cases for the hash_directory function"""

    def test_hash_directory_consistency(self, temp_files_for_hashing):
        """Test that hashing the same directory multiple times produces the same hash"""
        hash1 = fu.hash_directory(temp_files_for_hashing["test_dir"])
        hash2 = fu.hash_directory(temp_files_for_hashing["test_dir"])
        assert hash1 == hash2

    def test_hash_directory_different_content(self):
        """Test that different directory contents produce different hashes"""
        temp_dir = tempfile.mkdtemp()
        try:
            dir1 = os.path.join(temp_dir, "dir1")
            dir2 = os.path.join(temp_dir, "dir2")
            os.makedirs(dir1)
            os.makedirs(dir2)

            with open(os.path.join(dir1, "file.txt"), "w") as f:
                f.write("content1")
            with open(os.path.join(dir2, "file.txt"), "w") as f:
                f.write("content2")

            hash1 = fu.hash_directory(dir1)
            hash2 = fu.hash_directory(dir2)
            assert hash1 != hash2
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_hash_directory_raises_for_file(self, temp_files_for_hashing):
        """Test that ValueError is raised when trying to hash a file as directory"""
        with pytest.raises(ValueError, match="is not a valid directory"):
            fu.hash_directory(temp_files_for_hashing["test_file"])

    def test_hash_directory_raises_for_nonexistent(self):
        """Test that ValueError is raised for non-existent directory"""
        with pytest.raises(ValueError, match="is not a valid directory"):
            fu.hash_directory("/nonexistent/path/directory")

# ==================== TESTS: copy_file_to_objects ====================

class TestCopyFileToObjects:
    """Test cases for the copy_file_to_objects function"""

    def test_copy_file_successfully(self, temp_tracker_dir, temp_files_for_hashing):
        """Test successfully copying a file to objects directory and verify content"""
        file_hash = "abcdef123456"
        fu.copy_file_to_objects(temp_tracker_dir["tracker_path"],
                               temp_files_for_hashing["test_file"],
                               file_hash)

        expected_path = os.path.join(temp_tracker_dir["tracker_path"], "objects", file_hash)
        assert os.path.exists(expected_path)

        with open(expected_path, "r") as f:
            content = f.read()
        assert content == "known content for hashing"

    def test_copy_file_creates_parent_directories(self, temp_tracker_dir, temp_files_for_hashing):
        """Test that parent directories are created if they don't exist
         - Remove objects directory to check auto creation of parent directories
        """
        objects_dir = os.path.join(temp_tracker_dir["tracker_path"], "objects")
        shutil.rmtree(objects_dir)

        file_hash = "xyz789"
        fu.copy_file_to_objects(temp_tracker_dir["tracker_path"],
                               temp_files_for_hashing["test_file"],
                               file_hash)

        expected_path = os.path.join(temp_tracker_dir["tracker_path"], "objects", file_hash)
        assert os.path.exists(expected_path)

    def test_copy_directory_raises_error(self, temp_tracker_dir, temp_files_for_hashing):
        """Test that OSError is raised when trying to copy a directory"""
        with pytest.raises(OSError, match="Directory handling not implemented yet"):
            fu.copy_file_to_objects(temp_tracker_dir["tracker_path"],
                                   temp_files_for_hashing["test_dir"],
                                   "hash123")

# ==================== TESTS: cleanup_temp_files ====================

class TestCleanupTempFiles:
    """Test cases for the cleanup_temp_files function"""

    def test_cleanup_removes_dt_view_files(self, temp_file_structure, monkeypatch):
        """Test that cleanup_temp_files removes correct temporary files"""
        monkeypatch.setattr(tempfile, 'gettempdir', lambda: temp_file_structure["temp_dir"])

        removed, failed = fu.cleanup_temp_files()

        assert removed == 2
        assert failed == 0
        assert not os.path.exists(temp_file_structure["test_file1"])
        assert not os.path.exists(temp_file_structure["test_file2"])
        assert os.path.exists(temp_file_structure["other_file"])

    def test_cleanup_handles_errors_gracefully(self, temp_file_structure, monkeypatch):
        """Test that cleanup_temp_files handles errors gracefully"""
        monkeypatch.setattr(tempfile, 'gettempdir', lambda: temp_file_structure["temp_dir"])

        original_remove = os.remove

        def mock_remove(path):
            if path == temp_file_structure["test_file1"]:
                raise OSError("Mocked error")
            else:
                original_remove(path)

        monkeypatch.setattr(os, 'unlink', mock_remove)

        removed, failed = fu.cleanup_temp_files()

        assert removed == 1
        assert failed == 1
        assert os.path.exists(temp_file_structure["test_file1"])
        assert not os.path.exists(temp_file_structure["test_file2"])

    def test_cleanup_handles_directories(self, monkeypatch):
        """Test that cleanup_temp_files removes temporary directories"""
        temp_dir = tempfile.mkdtemp()
        try:
            dt_dir = os.path.join(temp_dir, "dt_view_test_dir")
            os.makedirs(dt_dir)

            monkeypatch.setattr(tempfile, 'gettempdir', lambda: temp_dir)

            removed, failed = fu.cleanup_temp_files()

            assert removed == 1
            assert failed == 0
            assert not os.path.exists(dt_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_cleanup_handles_listdir_error(self, monkeypatch):
        """Test that cleanup handles OSError when listing directory
         - function shouldn't crash, it should return zeros
        """
        def mock_listdir(path):
            raise OSError("Cannot list directory")

        monkeypatch.setattr(os, 'listdir', mock_listdir)

        removed, failed = fu.cleanup_temp_files()

        assert removed == 0
        assert failed == 0

# ==================== TESTS: display_structure ====================

class TestDisplayStructure:
    """Test cases for the display_structure function"""

    def test_display_single_file_structure(self, temp_tracker_dir):
        """Test displaying structure for a single file dataset"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            db.insert_object(conn, "hash1", 100)
            version_id = db.insert_version(conn, dataset_id, "hash1", 1.0, "/path/to/file.txt", "msg")
            db.insert_files(conn, version_id, "hash1", "file.txt")
            conn.commit()

        result = fu.display_structure(temp_tracker_dir["db_path"], dataset_id)

        assert "Structure:" in result
        assert "-file.txt" in result

    def test_display_directory_structure(self, temp_tracker_dir):
        """Test displaying structure for a directory dataset"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            db.insert_object(conn, "hash1", 100)
            db.insert_object(conn, "hash2", 200)
            version_id = db.insert_version(conn, dataset_id, "hash1", 1.0, "/path/to/mydir", "msg")
            db.insert_files(conn, version_id, "hash1", "file1.txt")
            db.insert_files(conn, version_id, "hash2", "subdir/file2.txt")
            conn.commit()

        result = fu.display_structure(temp_tracker_dir["db_path"], dataset_id)

        assert "Structure:" in result
        assert "mydir/" in result
        assert "file1.txt" in result
        assert "subdir/" in result
        assert "file2.txt" in result

    def test_display_structure_no_versions(self, temp_tracker_dir):
        """Test displaying structure when no versions exist"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            conn.commit()

        result = fu.display_structure(temp_tracker_dir["db_path"], dataset_id)

        assert "No versions found" in result

    def test_display_structure_specific_version(self, temp_tracker_dir):
        """Test displaying structure for a specific version"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            db.insert_object(conn, "hash1", 100)
            db.insert_object(conn, "hash2", 200)
            version_id1 = db.insert_version(conn, dataset_id, "hash1", 1.0, "/path/to/file.txt", "msg1")
            db.insert_files(conn, version_id1, "hash1", "file_v1.txt")
            version_id2 = db.insert_version(conn, dataset_id, "hash2", 2.0, "/path/to/file.txt", "msg2")
            db.insert_files(conn, version_id2, "hash2", "file_v2.txt")
            conn.commit()

        result = fu.display_structure(temp_tracker_dir["db_path"], dataset_id, version=1.0)

        assert "file_v1.txt" in result
        assert "file_v2.txt" not in result

# ==================== TESTS: open_file ====================

class TestOpenFile:
    """Test cases for the open_file function"""

    def test_open_file_raises_for_nonexistent(self):
        """Test that FileNotFoundError is raised for non-existent file"""
        with pytest.raises(FileNotFoundError, match="File not found"):
            fu.open_file("/nonexistent/file.txt")

    def test_open_file_windows(self, temp_files_for_hashing, monkeypatch):
        """Test opening file on Windows (mock os.startfile)"""
        if sys.platform != "win32":
            pytest.skip("Windows-specific test")

        opened_files = []

        def mock_startfile(path):
            opened_files.append(path)

        monkeypatch.setattr(os, 'startfile', mock_startfile)

        fu.open_file(temp_files_for_hashing["test_file"])

        assert len(opened_files) == 1
        assert opened_files[0] == temp_files_for_hashing["test_file"]

    def test_open_file_unix(self, temp_files_for_hashing, monkeypatch):
        """Test opening file on Unix-like systems (mock subprocess.run)"""
        if sys.platform == "win32":
            pytest.skip("Unix-specific test")

        called_commands = []

        def mock_run(cmd, check=False):
            called_commands.append(cmd)

        monkeypatch.setattr("subprocess.run", mock_run)

        fu.open_file(temp_files_for_hashing["test_file"])

        assert len(called_commands) == 1

# ==================== TESTS: open_dataset_version ====================

class TestOpenDatasetVersion:
    """Test cases for the open_dataset_version function"""

    def test_open_single_file_dataset(self, temp_tracker_dir, monkeypatch):
        """Test opening a single file dataset
         - Create test file in objects directory and insert dataset/version into database
         - Mock open_file to capture opened path instead of actually opening
        """
        test_hash = "testhash123"
        object_file = os.path.join(temp_tracker_dir["tracker_path"], "objects", test_hash)
        os.makedirs(os.path.dirname(object_file), exist_ok=True)
        with open(object_file, "w") as f:
            f.write("test content")

        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            db.insert_object(conn, test_hash, 100)
            version_id = db.insert_version(conn, dataset_id, test_hash, 1.0, "/path/file.csv", "msg")
            db.insert_files(conn, version_id, test_hash, "file.csv")
            conn.commit()

        opened_files = []
        def mock_open_file(path):
            opened_files.append(path)
        monkeypatch.setattr(fu, 'open_file', mock_open_file)

        success, msg = fu.open_dataset_version(dataset_id, None, 1.0)

        assert success
        assert len(opened_files) == 1
        assert "dt_view_" in opened_files[0]
        assert opened_files[0].endswith(".csv")

    def test_open_multi_file_dataset(self, temp_tracker_dir, monkeypatch):
        """Test opening a multi-file dataset (directory)
         - Create test files in objects directory
         - Insert dataset and version with multiple files into database
         - Mock open_file to capture opened paths instead of actually opening
         - Verify that the opened path is the temporary directory created for the version
        """
        hash1, hash2 = "hash1", "hash2"
        for h in [hash1, hash2]:
            object_file = os.path.join(temp_tracker_dir["tracker_path"], "objects", h)
            os.makedirs(os.path.dirname(object_file), exist_ok=True)
            with open(object_file, "w") as f:
                f.write(f"content for {h}")

        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            db.insert_object(conn, hash1, 100)
            db.insert_object(conn, hash2, 200)
            version_id = db.insert_version(conn, dataset_id, hash1, 1.0, "/path/mydir", "msg")
            db.insert_files(conn, version_id, hash1, "file1.txt")
            db.insert_files(conn, version_id, hash2, "subdir/file2.txt")
            conn.commit()

        opened_dirs = []
        def mock_open_file(path):
            opened_dirs.append(path)
        monkeypatch.setattr(fu, 'open_file', mock_open_file)

        success, msg = fu.open_dataset_version(dataset_id, None, 1.0)

        assert success
        assert len(opened_dirs) == 1
        assert "dt_view_" in opened_dirs[0]

    def test_open_dataset_tracker_not_initialized(self, monkeypatch):
        """Test opening dataset when tracker is not initialized"""
        monkeypatch.setattr(fu, 'find_data_tracker_root', lambda: None)

        success, msg = fu.open_dataset_version(1, None, 1.0)

        assert not success
        assert "not initialized" in msg

    def test_open_dataset_no_files_found(self, temp_tracker_dir):
        """Test opening dataset when no files are found for version"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            conn.commit()

        success, msg = fu.open_dataset_version(dataset_id, None, 1.0)

        assert not success
        assert "No files found" in msg

    def test_open_dataset_object_not_found(self, temp_tracker_dir):
        """Test opening dataset when object file doesn't exist"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            db.insert_object(conn, "nonexistent_hash", 100)
            version_id = db.insert_version(conn, dataset_id, "nonexistent_hash", 1.0, "/path/file.txt", "msg")
            db.insert_files(conn, version_id, "nonexistent_hash", "file.txt")
            conn.commit()

        success, msg = fu.open_dataset_version(dataset_id, None, 1.0)

        assert not success
        assert "error" in msg.lower()

# ==================== TESTS: export_file ====================

class TestExportFile:
    """Test cases for the export_file function"""

    def test_export_single_file(self, temp_tracker_dir):
        """Test exporting a single file dataset
         - Create test file in objects directory and insert dataset/version into database
         - Export to a temporary directory and verify the exported file content matches the original
        """
        temp_export_dir = tempfile.mkdtemp()
        try:
            test_hash = "exporthash1"
            object_file = os.path.join(temp_tracker_dir["tracker_path"], "objects", test_hash)
            os.makedirs(os.path.dirname(object_file), exist_ok=True)
            with open(object_file, "w") as f:
                f.write("export content")

            with db.open_database(temp_tracker_dir["db_path"]) as conn:
                dataset_id = db.insert_dataset(conn, "test_dataset", "message")
                db.insert_object(conn, test_hash, 100)
                version_id = db.insert_version(conn, dataset_id, test_hash, 1.0, "/path/file.txt", "msg")
                db.insert_files(conn, version_id, test_hash, "file.txt")
                conn.commit()

            export_path = os.path.join(temp_export_dir, "exported.txt")
            success, msg = fu.export_file(export_path, dataset_id, None, 1.0, False, False)

            assert success
            assert os.path.exists(export_path)
            with open(export_path, "r") as f:
                assert f.read() == "export content"
        finally:
            shutil.rmtree(temp_export_dir, ignore_errors=True)

    def test_export_multi_file_dataset(self, temp_tracker_dir):
        """Test exporting a multi-file dataset
         - Create test files in objects directory and insert dataset/version with multiple files into database
         - Export to a temporary directory and verify the exported files exist and have correct content
        """
        temp_export_dir = tempfile.mkdtemp()
        try:
            hash1, hash2 = "mhash1", "mhash2"
            for h, content in [(hash1, "content1"), (hash2, "content2")]:
                object_file = os.path.join(temp_tracker_dir["tracker_path"], "objects", h)
                os.makedirs(os.path.dirname(object_file), exist_ok=True)
                with open(object_file, "w") as f:
                    f.write(content)

            with db.open_database(temp_tracker_dir["db_path"]) as conn:
                dataset_id = db.insert_dataset(conn, "test_dataset", "message")
                db.insert_object(conn, hash1, 100)
                db.insert_object(conn, hash2, 200)
                version_id = db.insert_version(conn, dataset_id, hash1, 1.0, "/path/mydir", "msg")
                db.insert_files(conn, version_id, hash1, "file1.txt")
                db.insert_files(conn, version_id, hash2, "subdir/file2.txt")
                conn.commit()

            export_path = os.path.join(temp_export_dir, "exported_dir")
            success, msg = fu.export_file(export_path, dataset_id, None, 1.0, False, False)

            assert success
            assert os.path.exists(os.path.join(export_path, "file1.txt"))
            assert os.path.exists(os.path.join(export_path, "subdir", "file2.txt"))
        finally:
            shutil.rmtree(temp_export_dir, ignore_errors=True)

    def test_export_with_preserve_root(self, temp_tracker_dir):
        """Test exporting with --preserve-root flag"""
        temp_export_dir = tempfile.mkdtemp()
        try:
            test_hash = "phash1"
            object_file = os.path.join(temp_tracker_dir["tracker_path"], "objects", test_hash)
            os.makedirs(os.path.dirname(object_file), exist_ok=True)
            with open(object_file, "w") as f:
                f.write("content")

            with db.open_database(temp_tracker_dir["db_path"]) as conn:
                dataset_id = db.insert_dataset(conn, "test_dataset", "message")
                db.insert_object(conn, test_hash, 100)
                version_id = db.insert_version(conn, dataset_id, test_hash, 1.0, "/path/rootdir", "msg")
                db.insert_files(conn, version_id, test_hash, "file.txt")
                conn.commit()

            success, msg = fu.export_file(temp_export_dir, dataset_id, None, 1.0, False, True)

            assert success
            assert os.path.exists(os.path.join(temp_export_dir, "file.txt"))
        finally:
            shutil.rmtree(temp_export_dir, ignore_errors=True)

    def test_export_force_overwrite(self, temp_tracker_dir):
        """Test exporting with --force flag to overwrite existing file
         - Create an object file with known content and insert into database
         - Attempt to export to a path where a file already exists
         - First try without force - should fail and not overwrite
         - Then try with force - should succeed and overwrite the existing file
        """
        temp_export_dir = tempfile.mkdtemp()
        try:
            test_hash = "forcehash"
            object_file = os.path.join(temp_tracker_dir["tracker_path"], "objects", test_hash)
            os.makedirs(os.path.dirname(object_file), exist_ok=True)
            with open(object_file, "w") as f:
                f.write("new content")

            with db.open_database(temp_tracker_dir["db_path"]) as conn:
                dataset_id = db.insert_dataset(conn, "test_dataset", "message")
                db.insert_object(conn, test_hash, 100)
                version_id = db.insert_version(conn, dataset_id, test_hash, 1.0, "/path/file.txt", "msg")
                db.insert_files(conn, version_id, test_hash, "file.txt")
                conn.commit()

            export_path = os.path.join(temp_export_dir, "file.txt")
            with open(export_path, "w") as f:
                f.write("old content")

            success, msg = fu.export_file(export_path, dataset_id, None, 1.0, False, False)
            assert not success
            assert "already exists" in msg

            success, msg = fu.export_file(export_path, dataset_id, None, 1.0, True, False)
            assert success
            with open(export_path, "r") as f:
                assert f.read() == "new content"
        finally:
            shutil.rmtree(temp_export_dir, ignore_errors=True)

    def test_export_empty_path(self, temp_tracker_dir):
        """Test that empty export path returns error"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            conn.commit()

        success, msg = fu.export_file("", dataset_id, None, 1.0, False, False)
        assert not success
        assert "cannot be empty" in msg

    def test_export_tracker_not_initialized(self, monkeypatch):
        """Test exporting when tracker is not initialized"""
        monkeypatch.setattr(fu, 'find_data_tracker_root', lambda: None)

        success, msg = fu.export_file("/some/path", 1, None, 1.0, False, False)
        assert not success
        assert "not initialized" in msg

    def test_export_no_files_found(self, temp_tracker_dir):
        """Test exporting when no files found for version"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            conn.commit()

        success, msg = fu.export_file("/some/path", dataset_id, None, 1.0, False, False)
        assert not success
        assert "No files found" in msg

    def test_export_object_not_found(self, temp_tracker_dir):
        """Test exporting when object file doesn't exist in objects directory"""
        with db.open_database(temp_tracker_dir["db_path"]) as conn:
            dataset_id = db.insert_dataset(conn, "test_dataset", "message")
            db.insert_object(conn, "missing_hash", 100)
            version_id = db.insert_version(conn, dataset_id, "missing_hash", 1.0, "/path/file.txt", "msg")
            db.insert_files(conn, version_id, "missing_hash", "file.txt")
            conn.commit()

        success, msg = fu.export_file("/some/path", dataset_id, None, 1.0, False, False)
        assert not success
        assert "not found" in msg

# -------------------- TESTS: get_storage_stats -----------------------

class TestGetStorageStats:
    """Test cases for the get_storage_stats function"""

    def test_get_stats_with_files(self, temp_tracker_dir):
        """Test getting storage stats when objects directory has files
         - Create some test files in objects directory
        """
        objects_dir = os.path.join(temp_tracker_dir["tracker_path"], "objects")
        for i in range(3):
            file_path = os.path.join(objects_dir, f"testfile{i}")
            with open(file_path, "w") as f:
                f.write("x" * 100)  # 100 bytes each

        success, msg = fu.get_storage_stats()

        assert success
        assert "3" in msg  # 3 files
        assert "300.00 B" in msg  # 300 bytes total

    def test_get_stats_empty_objects(self, temp_tracker_dir):
        """Test getting storage stats when objects directory is empty"""
        success, msg = fu.get_storage_stats()

        assert success
        assert "0" in msg
        assert "0.00 B" in msg

    def test_get_stats_tracker_not_initialized(self, monkeypatch):
        """Test getting stats when tracker is not initialized"""
        monkeypatch.setattr(fu, 'find_data_tracker_root', lambda: None)

        success, msg = fu.get_storage_stats()

        assert not success
        assert "not initialized" in msg

