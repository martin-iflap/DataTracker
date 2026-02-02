from data_tracker import db_manager as db
import tempfile
import pytest
import time
import os


@pytest.fixture()
def test_db_path():
    """Provide a temporary database file path"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
        db_path = tmp.name
    yield db_path
    if os.path.exists(db_path):
        time.sleep(0.05)  # Small delay to ensure connection is closed on Windows
        try:
            os.remove(db_path)
        except PermissionError:
            pass

@pytest.fixture()
def in_memory_db(test_db_path):
    """Create an initialized test database connection"""
    success, msg = db.initialize_database(test_db_path)
    assert success, f"Database initialization failed: {msg}"

    conn = db.open_database(test_db_path)
    yield conn
    conn.close()

@pytest.fixture()
def dataset_with_version(in_memory_db):
    """Fixture providing a dataset with one version for testing"""
    dataset_id = db.insert_dataset(in_memory_db, "test-dataset", "Test notes")
    db.insert_object(in_memory_db, "abc123", 1000)
    version_id = db.insert_version(
        in_memory_db, dataset_id, "abc123", 1.0, "/path/to/data", "Initial version"
    )

    return {
        'conn': in_memory_db,
        'dataset_id': dataset_id,
        'version_id': version_id,
        'object_hash': "abc123"
    }


# ============================================================================
# INITIALIZATION TESTS
# ============================================================================

class TestInitialization:
    """Test database initialization"""

    def test_initialize_database_creates_tables(self, test_db_path):
        """Test that initialize_database creates all required tables"""
        success, msg = db.initialize_database(test_db_path)
        assert success is True
        assert "initialized successfully" in msg

        with db.open_database(test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row['name'] for row in cursor.fetchall()]
            assert tables == ['datasets', 'files', 'objects', 'versions'] # tables contains also sqlite_sequence

    def test_initialize_database_enables_foreign_keys(self, test_db_path):
        """Test that foreign keys are enabled"""
        db.initialize_database(test_db_path)

        with db.open_database(test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys")
            assert cursor.fetchone()[0] == 1

    def test_initialize_database_is_idempotent(self, test_db_path):
        """Test that running initialize_database twice doesn't fail"""
        success1, msg1 = db.initialize_database(test_db_path)
        success2, msg2 = db.initialize_database(test_db_path)

        assert success1 is True
        assert success2 is True


# ============================================================================
# DATASET OPERATIONS
# ============================================================================

class TestDatasetOperations:
    """Test dataset CRUD operations"""

    def test_insert_dataset_with_name(self, in_memory_db):
        """Test inserting a dataset with explicit name"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", "Test notes")

        assert isinstance(dataset_id, int)
        assert dataset_id == 1

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT name, notes FROM datasets WHERE id = ?", (dataset_id,))
        row = cursor.fetchone()
        assert row['name'] == "test-dataset"
        assert row['notes'] == "Test notes"

    def test_insert_dataset_without_name(self, in_memory_db):
        """Test inserting a dataset with auto-generated name"""
        dataset_id = db.insert_dataset(in_memory_db, None, "Test notes")

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT name FROM datasets WHERE id = ?", (dataset_id,))
        row = cursor.fetchone()
        assert row['name'] == f"dataset-{dataset_id}"

    def test_dataset_exists_by_id(self, in_memory_db):
        """Test checking if dataset exists by ID"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)

        assert db.dataset_exists(in_memory_db, dataset_id, None) is True
        assert db.dataset_exists(in_memory_db, 999, None) is False

    def test_dataset_exists_by_name(self, in_memory_db):
        """Test checking if dataset exists by name"""
        db.insert_dataset(in_memory_db, "test-dataset", None)

        assert db.dataset_exists(in_memory_db, None, "test-dataset") is True
        assert db.dataset_exists(in_memory_db, None, "nonexistent") is False

    def test_get_id_from_name(self, in_memory_db):
        """Test retrieving dataset ID by name"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)
        found_id = db.get_id_from_name(in_memory_db, "test-dataset")

        assert found_id == dataset_id

    def test_get_id_from_name_nonexistent(self, in_memory_db):
        """Test that getting ID of nonexistent dataset raises error"""
        with pytest.raises(ValueError, match="Dataset with name 'nonexistent' does not exist"):
            db.get_id_from_name(in_memory_db, "nonexistent")

    def test_delete_dataset(self, in_memory_db):
        """Test deleting a dataset"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)
        db.delete_dataset(in_memory_db, dataset_id)

        assert db.dataset_exists(in_memory_db, dataset_id, None) is False


# ============================================================================
# OBJECT OPERATIONS
# ============================================================================

class TestObjectOperations:
    """Test object storage operations"""

    def test_insert_object(self, in_memory_db):
        """Test inserting an object"""
        db.insert_object(in_memory_db, "abc123", 1024)

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT hash, size FROM objects WHERE hash = ?", ("abc123",))
        obj = cursor.fetchone()

        assert obj is not None
        assert obj['hash'] == "abc123"
        assert obj['size'] == 1024

    def test_insert_object_duplicate_ignored(self, in_memory_db):
        """Test that duplicate objects are ignored (INSERT OR IGNORE)"""
        db.insert_object(in_memory_db, "abc123", 1000)
        db.insert_object(in_memory_db, "abc123", 2000)  # Different size, should be ignored

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT COUNT(*) as count, size FROM objects WHERE hash = ?", ("abc123",))
        row = cursor.fetchone()

        assert row['count'] == 1
        assert row['size'] == 1000  # Original size preserved

    def test_object_is_used(self, dataset_with_version):
        """Test checking if object is referenced by versions"""
        conn = dataset_with_version['conn']
        object_hash = dataset_with_version['object_hash']

        assert db.object_is_used(conn, object_hash) is True
        assert db.object_is_used(conn, "unused_hash") is False

    def test_delete_unused_objects(self, in_memory_db):
        """Test deleting objects not referenced by any files"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)

        # Add objects
        db.insert_object(in_memory_db, "used_hash", 1000)
        db.insert_object(in_memory_db, "unused_hash", 2000)

        # Add version + file referencing "used_hash"
        version_id = db.insert_version(in_memory_db, dataset_id, "used_hash", 1.0, "/path", None)
        db.insert_files(in_memory_db, version_id, "used_hash", "file.txt")

        # Delete unused objects
        deleted = db.delete_object(in_memory_db)

        assert "unused_hash" in deleted
        assert "used_hash" not in deleted


# ============================================================================
# VERSION OPERATIONS
# ============================================================================

class TestVersionOperations:
    """Test version tracking operations"""

    def test_insert_version(self, in_memory_db):
        """Test inserting a new version"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)
        db.insert_object(in_memory_db, "abc123", 1000)

        version_id = db.insert_version(
            in_memory_db, dataset_id, "abc123", 1.0, "/path/to/data", "Initial version"
        )

        assert version_id == 1

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT * FROM versions WHERE id = ?", (version_id,))
        version = cursor.fetchone()

        assert version['version'] == 1.0  # ✓ Fixed column name
        assert version['original_path'] == "/path/to/data"  # ✓ Fixed column name
        assert version['object_hash'] == "abc123"
        assert version['message'] == "Initial version"

    def test_check_version_exists(self, dataset_with_version):
        """Test checking if a version exists"""
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']

        assert db.check_version_exists(conn, dataset_id, 1.0) is True
        assert db.check_version_exists(conn, dataset_id, 2.0) is False

    def test_get_latest_version_empty_dataset(self, in_memory_db):
        """Test getting latest version for dataset with no versions"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)

        assert db.get_latest_version(in_memory_db, dataset_id) == 0.0

    def test_get_latest_version_with_versions(self, dataset_with_version):
        """Test getting latest version for dataset with multiple versions"""
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']

        # Add more versions
        db.insert_object(conn, "def456", 2000)
        db.insert_version(conn, dataset_id, "def456", 1.5, "/path2", None)
        db.insert_object(conn, "ghi789", 3000)
        db.insert_version(conn, dataset_id, "ghi789", 2.0, "/path3", None)

        assert db.get_latest_version(conn, dataset_id) == 2.0

    def test_delete_versions(self, dataset_with_version):
        """Test deleting all versions for a dataset"""
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']

        db.delete_versions(conn, dataset_id)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM versions WHERE dataset_id = ?", (dataset_id,))
        assert cursor.fetchone()['count'] == 0


# ============================================================================
# FILE OPERATIONS
# ============================================================================

class TestFileOperations:
    """Test file tracking operations"""

    def test_insert_files(self, dataset_with_version):
        """Test inserting files associated with a version"""
        conn = dataset_with_version['conn']
        version_id = dataset_with_version['version_id']

        db.insert_files(conn, version_id, "file_hash", "relative/path/file.txt")

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM files WHERE version_id = ?", (version_id,))
        file_row = cursor.fetchone()

        assert file_row is not None
        assert file_row['object_hash'] == "file_hash"
        assert file_row['relative_path'] == "relative/path/file.txt"

    def test_delete_files(self, dataset_with_version):
        """Test deleting all files for a dataset"""
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']
        version_id = dataset_with_version['version_id']

        # Add files
        db.insert_files(conn, version_id, "hash1", "file1.txt")
        db.insert_files(conn, version_id, "hash2", "file2.txt")

        # Delete
        db.delete_files(conn, dataset_id)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM files WHERE version_id = ?", (version_id,))
        assert cursor.fetchone()['count'] == 0
