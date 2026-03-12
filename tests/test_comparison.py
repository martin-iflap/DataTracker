from data_tracker import comparison as comp
from data_tracker import db_manager as db
from data_tracker import core
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
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    file1.write_text("line1\nline2\n")
    file2.write_text("line1\nline2\n")

    similarity, added, removed = comp.compare_files(str(file1), str(file2))

    assert similarity == 100.0
    assert added == 0
    assert removed == 0

def test_compare_text_files(tmp_path):
    """Test the compare_files function with two text files that have some differences
     - create two files with the same content except for one line,
       and check that the similarity is between 0 and 100 and that
       the added and removed lines are correctly counted
    """
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    file1.write_text("line1\nline2\n")
    file2.write_text("line1\nmodified\n")

    similarity, added, removed = comp.compare_files(str(file1), str(file2))

    assert 0 < similarity < 100
    assert added == 1
    assert removed == 1

def test_compare_binary_files(tmp_path):
    """Test the compare_files function with two slightly different binary files
     - create two binary files with the same content except for one byte
     - check 0 < similarity < 100 and added and removed are None
    """
    file1 = tmp_path / "binary1.bin"
    file2 = tmp_path / "binary2.bin"
    file1.write_bytes(b'\x00\x01\x02\x03')
    file2.write_bytes(b'\x00\x01\xFF\x03')

    similarity, added, removed = comp.compare_files(str(file1), str(file2))

    assert similarity == 75.0
    assert added is None
    assert removed is None

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
        assert "Cannot compare a version to itself" in message

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

    def test_compare_with_invalid_version(self, tmp_path):
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


class TestDiffDataset:
    """Test the diff_dataset function.
    Uses real files on disk tracked via core.add_data so that original_path
    in the DB points to an actual file.
    """

    @staticmethod
    def _add_dataset(temp_tracker_dir, file_path: str, content: str,
                     version: float = 1.0, message: str = "v1") -> int:
        """Write content to file_path, track it, return dataset_id."""
        with open(file_path, 'w') as f:
            f.write(content)
        success, msg = core.add_data(file_path, "test-dataset", version, message)
        assert success, f"add_data failed: {msg}"
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            return db.get_id_from_name(conn, "test-dataset")

    def test_diff_no_tracker(self, monkeypatch):
        """diff_dataset returns failure when no tracker is found."""
        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root', lambda: None)
        success, message = comp.diff_dataset(1, None, None)
        assert success is False
        assert "not initialized" in message

    def test_diff_no_versions(self, temp_tracker_dir):
        """diff_dataset returns failure when dataset has no versions."""
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "empty-dataset", None)
            conn.commit()
        success, message = comp.diff_dataset(dataset_id, None, None)
        assert success is False
        assert "No versions found" in message

    def test_diff_file_up_to_date(self, temp_tracker_dir, tmp_path):
        """diff_dataset reports no differences when live file matches stored version."""
        file_path = str(tmp_path / "data.csv")
        dataset_id = self._add_dataset(temp_tracker_dir, file_path, "col1,col2\n1,2\n")

        success, message = comp.diff_dataset(dataset_id, None, None)
        assert success is True
        assert "No differences" in message

    def test_diff_file_modified(self, temp_tracker_dir, tmp_path):
        """diff_dataset detects when the live file has been modified since last version."""
        file_path = str(tmp_path / "data.csv")
        dataset_id = self._add_dataset(temp_tracker_dir, file_path, "col1,col2\n1,2\n")

        # Modify live file after tracking
        with open(file_path, 'w') as f:
            f.write("col1,col2\n1,2\n3,4\n")

        success, message = comp.diff_dataset(dataset_id, None, None)
        assert success is True
        assert "Modified files" in message
        assert "data.csv" in message

    def test_diff_live_file_missing(self, temp_tracker_dir, tmp_path):
        """diff_dataset returns a clear error when the live file no longer exists."""
        file_path = str(tmp_path / "data.csv")
        dataset_id = self._add_dataset(temp_tracker_dir, file_path, "col1,col2\n1,2\n")

        os.remove(file_path)

        success, message = comp.diff_dataset(dataset_id, None, None)
        assert success is False
        assert "not found" in message.lower()

    def test_diff_specific_version(self, temp_tracker_dir, tmp_path):
        """diff_dataset diffs against the specified version, not just latest."""
        file_path = str(tmp_path / "data.csv")

        # v1
        with open(file_path, 'w') as f:
            f.write("col1\n1\n")
        success, msg = core.add_data(file_path, "test-dataset", 1.0, "v1")
        assert success, msg

        # v2 — update with different content
        with open(file_path, 'w') as f:
            f.write("col1\n1\n2\n")
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.get_id_from_name(conn, "test-dataset")
        success, msg = core.update_data(file_path, dataset_id, None, 2.0, "v2")
        assert success, msg

        # live file now matches v2 — diffing against v1 should show differences
        success, message = comp.diff_dataset(dataset_id, None, 1.0)
        assert success is True
        assert "Modified files" in message

    def test_diff_by_name(self, temp_tracker_dir, tmp_path):
        """diff_dataset resolves dataset by name when id is None."""
        file_path = str(tmp_path / "data.csv")
        self._add_dataset(temp_tracker_dir, file_path, "a,b\n1,2\n")

        success, message = comp.diff_dataset(None, "test-dataset", None)
        assert success is True
        assert "No differences" in message

    def test_diff_invalid_version(self, temp_tracker_dir, tmp_path):
        """diff_dataset returns failure when specified version does not exist."""
        file_path = str(tmp_path / "data.csv")
        dataset_id = self._add_dataset(temp_tracker_dir, file_path, "a,b\n1,2\n")

        success, message = comp.diff_dataset(dataset_id, None, 99.0)
        assert success is False
        assert "No files found" in message

    def test_diff_directory_with_nested_structure(self, temp_tracker_dir, tmp_path):
        """diff_dataset correctly detects changes in a tracked directory.
        Builds a real nested folder structure, tracks it, modifies one file
        and adds another, then verifies diff reports both changes.
        """
        dataset_dir = tmp_path / "my_dataset"
        sub_dir = dataset_dir / "subdir"
        sub_dir.mkdir(parents=True)

        (dataset_dir / "root_file.csv").write_text("id,value\n1,100\n")
        (sub_dir / "nested_file.csv").write_text("id,value\n2,200\n")

        success, msg = core.add_data(str(dataset_dir), "dir-dataset", 1.0, "initial")
        assert success, f"add_data failed: {msg}"

        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.get_id_from_name(conn, "dir-dataset")

        # Modify one existing file and add a new one
        (dataset_dir / "root_file.csv").write_text("id,value\n1,100\n2,999\n")
        (sub_dir / "new_file.csv").write_text("id,value\n3,300\n")

        success, message = comp.diff_dataset(dataset_id, None, None)

        assert success is True
        assert "Modified files" in message
        assert "root_file.csv" in message
        assert "Added files" in message
        assert "new_file.csv" in message
        assert "No files removed." in message

        shutil.rmtree(str(dataset_dir), ignore_errors=True)
