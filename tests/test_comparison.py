from data_tracker import comparison as comp
from data_tracker import db_manager as db
import shutil
import os



def create_object_file(tracker_path: str, file_hash: str, content: str):
    """Helper to create an object file with specific content"""
    objects_path = os.path.join(tracker_path, "objects")
    file_path = os.path.join(objects_path, file_hash)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

def create_dataset_version(db_path: str, tracker_path: str, dataset_id: int,
                          version: float, files_dict: dict):
    """Helper to create a dataset version with files.

    Args:
        db_path: Path to tracker.db
        tracker_path: Path to .data_tracker directory
        dataset_id: ID of the dataset
        version: Version number
        files_dict: Dictionary mapping relative_path -> (hash, size, content)
                   Example: {"file1.txt": ("hash1", 100, "content")}
    """
    with db.open_database(db_path) as conn:
        # Insert objects and create object files
        for rel_path, (file_hash, size, content) in files_dict.items():
            db.insert_object(conn, file_hash, size)
            create_object_file(tracker_path, file_hash, content)

        # Get primary hash (use first file's hash)
        primary_hash = list(files_dict.values())[0][0]

        # Insert version
        version_id = db.insert_version(
            conn, dataset_id, primary_hash, version,
            f"C:\\test\\data\\v{version}", f"Version {version}"
        )

        # Insert all files
        for rel_path, (file_hash, size, content) in files_dict.items():
            db.insert_files(conn, version_id, file_hash, rel_path)

        conn.commit()

# ---------------------------------    TESTS    ------------------------------------

def test_compare_identical_files(tmp_path):
    """Test the compare_files function with two identical text files
     - similarity should be 100%, added and removed lines should be 0
    """
    objects_dir = tmp_path / "objects"
    objects_dir.mkdir()

    hash1 = "abc123"
    hash2 = "def456"
    (objects_dir / hash1).write_text("line1\nline2\n")
    (objects_dir / hash2).write_text("line1\nline2\n")

    similarity, added, removed = comp.compare_files(str(tmp_path), hash1, hash2)

    assert similarity == 100.0
    assert added == 0
    assert removed == 0

    try:
        shutil.rmtree(str(objects_dir), ignore_errors=True)
    except:
        raise

def test_compare_text_files(tmp_path):
    """Test the compare_files function with two text files that have some differences
     - create two files with the same content except for one line,
       and check that the similarity is between 0 and 100 and that
       the added and removed lines are correctly counted
    """
    objects_dir = tmp_path / "objects"
    objects_dir.mkdir()

    hash1 = "abc123"
    hash2 = "def456"
    (objects_dir / hash1).write_text("line1\nline2\n")
    (objects_dir / hash2).write_text("line1\nmodified\n")

    similarity, added, removed = comp.compare_files(str(tmp_path), hash1, hash2)

    assert 0 < similarity < 100
    assert added == 1
    assert removed == 1

    try:
        shutil.rmtree(str(objects_dir), ignore_errors=True)
    except:
        raise

def test_compare_binary_files(tmp_path):
    """Test the compare_files function with two slightly different binary files
     - create two binary files with the same content except for one byte
     - check 0 < similarity < 100 and added and removed are None
    """
    objects_dir = tmp_path / "objects"
    objects_dir.mkdir()

    hash1 = "binary1"
    hash2 = "binary2"
    (objects_dir / hash1).write_bytes(b'\x00\x01\x02\x03')
    (objects_dir / hash2).write_bytes(b'\x00\x01\xFF\x03')

    similarity, added, removed = comp.compare_files(str(tmp_path), hash1, hash2)

    assert similarity == 75.0
    assert added is None
    assert removed is None

    try:
        shutil.rmtree(str(objects_dir), ignore_errors=True)
    except:
        raise

# --------------------------    TESTS FOR compare_dataset_versions -------------------

class TestCompareDatasetVersions:
    """Comprehensive tests for the compare_dataset_versions function"""

    def test_compare_tracker_not_initialized(self, monkeypatch):
        """Test comparison fails when tracker is not initialized"""
        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root', lambda: None)

        success, message = comp.compare_dataset_versions(1, None, 1.0, 2.0)

        assert success is False
        assert "not initialized" in message

    def test_compare_same_version(self, temp_tracker_dir):
        """Test comparison fails when comparing same version to itself"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        create_dataset_version( # create a single version only
            db_path, tracker_path, dataset_id, 1.0,
            {"file1.txt": ("hash1", 100, "content1")}
        )

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 1.0)

        assert success is False
        assert "Cannot compare the same version" in message

    def test_compare_auto_detect_versions(self, temp_tracker_dir):
        """Test that auto-detection compares the two most recent versions, not first and latest.
         - create 3 versions, then call compare with both versions as None
         - should compare versions 2.0 and 3.0, not 1.0 and 3.0
         - check that the message indicates the correct versions being compared
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        create_dataset_version(
            db_path, tracker_path, dataset_id, 1.0,
            {"file1.txt": ("hash1", 100, "content v1")}
        )
        create_dataset_version(
            db_path, tracker_path, dataset_id, 2.0,
            {"file1.txt": ("hash2", 100, "content v2")}
        )
        create_dataset_version(
            db_path, tracker_path, dataset_id, 3.0,
            {"file1.txt": ("hash3", 100, "content v3")}
        )

        success, message = comp.compare_dataset_versions(dataset_id, None, None, None)

        assert success is True
        assert "Comparison between version 2.0 and version 3.0" in message
        assert "1.0" not in message.split("Comparison")[1].split("\n")[0]

    def test_compare_no_differences(self, temp_tracker_dir):
        """Test comparison when two versions have identical files
         - create two versions with the same file paths, hashes, sizes, and content
         - should return success True and message indicating no differences
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        files_v1 = {
            "file1.txt": ("hash1", 100, "identical content"),
            "file2.txt": ("hash2", 200, "more content")
        }
        files_v2 = {
            "file1.txt": ("hash1", 100, "identical content"),
            "file2.txt": ("hash2", 200, "more content")
        }

        create_dataset_version(db_path, tracker_path, dataset_id, 1.0, files_v1)
        create_dataset_version(db_path, tracker_path, dataset_id, 2.0, files_v2)

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 2.0)

        assert success is True
        assert "No differences between version 1.0 and version 2.0" in message

    def test_compare_added_files_only(self, temp_tracker_dir):
        """Test comparison when files are only added in v2"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        files_v1 = {
            "file1.txt": ("hash1", 100, "content1"),
            "file2.txt": ("hash2", 200, "content2")
        }
        files_v2 = {
            "file1.txt": ("hash1", 100, "content1"),
            "file2.txt": ("hash2", 200, "content2"),
            "file3.txt": ("hash3", 300, "content3")
        }

        create_dataset_version(db_path, tracker_path, dataset_id, 1.0, files_v1)
        create_dataset_version(db_path, tracker_path, dataset_id, 2.0, files_v2)

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 2.0)

        assert success is True
        assert "Added files:" in message
        assert "file3.txt" in message
        assert "300.00 B" in message or "300 B" in message
        assert "No files removed." in message
        assert "No files modified." in message

    def test_compare_removed_files_only(self, temp_tracker_dir):
        """Test comparison when files are only removed in v2"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        files_v1 = {
            "file1.txt": ("hash1", 100, "content1"),
            "file2.txt": ("hash2", 200, "content2"),
            "file3.txt": ("hash3", 300, "content3")
        }
        files_v2 = {
            "file1.txt": ("hash1", 100, "content1"),
            "file2.txt": ("hash2", 200, "content2")
        }

        create_dataset_version(db_path, tracker_path, dataset_id, 1.0, files_v1)
        create_dataset_version(db_path, tracker_path, dataset_id, 2.0, files_v2)

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 2.0)

        assert success is True
        assert "Removed files:" in message
        assert "file3.txt" in message
        assert "No files added." in message
        assert "No files modified." in message

    def test_compare_modified_files_only(self, temp_tracker_dir):
        """Test comparison when files are only modified (same path, different content)
         - Add the same file with different content
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        files_v1 = {
            "file1.txt": ("hash1", 100, "old content\nline2\nline3")
        }
        files_v2 = {
            "file1.txt": ("hash2", 150, "new content\nline2\nline3")
        }

        create_dataset_version(db_path, tracker_path, dataset_id, 1.0, files_v1)
        create_dataset_version(db_path, tracker_path, dataset_id, 2.0, files_v2)

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 2.0)

        assert success is True
        assert "Modified files:" in message
        assert "file1.txt" in message
        assert "Similarity:" in message
        assert "Lines added:" in message
        assert "Lines removed:" in message
        assert "No files added." in message
        assert "No files removed." in message

    def test_compare_mixed_changes(self, temp_tracker_dir):
        """Test comparison with added, removed, and modified files"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        # v1: file1, file2, file3
        # v2: file1 (modified), file2 (unchanged), file4 (new)
        # Result: file1 modified, file3 removed, file4 added
        files_v1 = {
            "file1.txt": ("hash1", 100, "old content"),
            "file2.txt": ("hash2", 200, "unchanged"),
            "file3.txt": ("hash3", 300, "removed content")
        }
        files_v2 = {
            "file1.txt": ("hash1_new", 120, "new content"),
            "file2.txt": ("hash2", 200, "unchanged"),
            "file4.txt": ("hash4", 400, "added content")
        }

        create_dataset_version(db_path, tracker_path, dataset_id, 1.0, files_v1)
        create_dataset_version(db_path, tracker_path, dataset_id, 2.0, files_v2)

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 2.0)

        assert success is True
        assert "Modified files:" in message
        assert "file1.txt" in message
        assert "Added files:" in message
        assert "file4.txt" in message
        assert "Removed files:" in message
        assert "file3.txt" in message

    def test_compare_with_size_calculations(self, temp_tracker_dir):
        """Test that size calculations are accurate"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        files_v1 = {
            "file1.txt": ("hash1", 1024, "a" * 1024),  # 1 KB
            "file2.txt": ("hash2", 2048, "b" * 2048)   # 2 KB
        }
        files_v2 = {
            "file1.txt": ("hash1", 1024, "a" * 1024),  # 1 KB (unchanged)
            "file3.txt": ("hash3", 3072, "c" * 3072)   # 3 KB (added)
        }

        create_dataset_version(db_path, tracker_path, dataset_id, 1.0, files_v1)
        create_dataset_version(db_path, tracker_path, dataset_id, 2.0, files_v2)

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 2.0)

        assert success is True # file2 removed (2048 bytes = 2 KB)
        assert "Total size removed:" in message # file3 added (3072 bytes = 3 KB)
        assert "Total size added:" in message

    def test_compare_structure_display(self, temp_tracker_dir):
        """Test that nested structure display is included in output"""
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()

        files_v1 = {
            "dir1/file1.txt": ("hash1", 100, "content1"),
            "dir1/file2.txt": ("hash2", 200, "content2")
        }
        files_v2 = {
            "dir1/file1.txt": ("hash1", 100, "content1"),
            "dir2/file3.txt": ("hash3", 300, "content3")
        }

        create_dataset_version(db_path, tracker_path, dataset_id, 1.0, files_v1)
        create_dataset_version(db_path, tracker_path, dataset_id, 2.0, files_v2)

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 2.0)

        assert success is True
        assert "Version: 1.0" in message
        assert "Version: 2.0" in message
        assert "Structure:" in message

    def test_compare_empty_dataset_error(self, temp_tracker_dir):
        """Test error when dataset has no versions and auto-detection is attempted
         - create an empty dataset with no versions, then call compare with both versions as None
         - should fail as fewer than 2 versions exist
        """
        db_path = temp_tracker_dir['db_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "empty-dataset", None)
            conn.commit()

        success, message = comp.compare_dataset_versions(dataset_id, None, None, 2.0)
        assert success is False
        assert "Could not determine first version" in message or "needs at least 2 versions" in message

        success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, None)
        assert success is False
        assert "Could not determine latest version" in message

    def test_compare_single_version_auto_detect_fails(self, temp_tracker_dir):
        """Test that auto-detection fails gracefully when dataset has only one version.
         - create a dataset with only one version, then call compare with both versions as None
         - should fail as compare needs at least 2 versions for auto-detection
        """
        db_path = temp_tracker_dir['db_path']
        tracker_path = temp_tracker_dir['tracker_path']

        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "single-version-dataset", None)
            conn.commit()

        create_dataset_version(
            db_path, tracker_path, dataset_id, 1.0,
            {"file1.txt": ("hashA", 100, "only version")}
        )

        success, message = comp.compare_dataset_versions(dataset_id, None, None, None)

        assert success is False
        assert "needs at least 2 versions" in message


def test_compare_with_invalid_version(tmp_path):
    """Test comparison fails gracefully with invalid version number
     - use simpler setup with direct DB manipulation to create a dataset
       and then attempt to compare with a version that doesn't exist
    """
    db_path = tmp_path / "tracker.db"
    db.initialize_database(str(db_path))

    with db.open_database(str(db_path)) as conn:
        dataset_id = db.insert_dataset(conn, "test-dataset", None)
        conn.commit()

    success, message = comp.compare_dataset_versions(dataset_id, None, 1.0, 999.0)

    assert success is False
    assert "No files found" in message or "invalid" in message.lower()

    try:
        shutil.rmtree(str(db_path), ignore_errors=True)
    except:
        raise
