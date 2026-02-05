from data_tracker import db_manager as db
import tracemalloc
import tempfile
import pytest
import shutil
import os


tracemalloc.start()

@pytest.fixture()
def temp_db_dir():
    """Create temporary directory for database files"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    try:
        shutil.rmtree(temp_dir, ignore_errors=True)
    except:
        raise

@pytest.fixture()
def test_db_path(temp_db_dir) -> str:
    """Provide a temporary database file path"""
    db_path = os.path.join(temp_db_dir, "test_db")
    return db_path

@pytest.fixture()
def in_memory_db(test_db_path):
    """Create an initialized test database connection"""
    success, msg = db.initialize_database(test_db_path)
    assert success, f"Database initialization failed: {msg}"

    with db.open_database(test_db_path) as conn:
        yield conn
        conn.close()

@pytest.fixture()
def in_memory_db_no_connection(test_db_path) -> str:
    """Create an initialized test database without opening a connection
     - useful for testing functions that open their own connections
    """
    success, msg = db.initialize_database(test_db_path)
    assert success, f"Database initialization failed: {msg}"
    return test_db_path

@pytest.fixture()
def dataset_with_version(in_memory_db) -> dict:
    """Fixture providing a dataset with one version for testing with open connection"""
    dataset_id = db.insert_dataset(in_memory_db, "test-dataset", "Test notes")
    db.insert_object(in_memory_db, "abc123", 1000)
    version_id = db.insert_version(
        in_memory_db, dataset_id, "abc123", 1.0, "C:\\path\\to\\data", "Initial version"
    )

    return {
        'conn': in_memory_db,
        'dataset_id': dataset_id,
        'version_id': version_id,
        'object_hash': "abc123"
    }

@pytest.fixture()
def dataset_with_version_no_conn(in_memory_db_no_connection) -> dict:
    """Fixture providing a dataset with one version for testing without open connection
     - useful for testing functions that open their own connections
    """
    with db.open_database(in_memory_db_no_connection) as conn:
        dataset_id = db.insert_dataset(conn, "test-dataset", "Test notes")
        db.insert_object(conn, "abc123", 1000)
        version_id = db.insert_version(
            conn, dataset_id, "abc123", 1.0, "C:\\path\\to\\data", "Initial version"
        )
        conn.commit()

        return {
            'db_path': in_memory_db_no_connection,
            'dataset_id': dataset_id,
            'version_id': version_id,
            'object_hash': "abc123"
        }

# ---------------------- INITIALIZATION TESTS ----------------------

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
            assert tables == ['datasets', 'files', 'objects', 'sqlite_sequence', 'versions']

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

# ------------------ DATASET OPERATIONS ------------------

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

    def test_get_dataset_name_from_id(self, in_memory_db_no_connection):
        """Test retrieving dataset name by ID"""
        with db.open_database(in_memory_db_no_connection) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            conn.commit()
        found_name = db.get_dataset_name_from_id(in_memory_db_no_connection, dataset_id)

        assert found_name == "test-dataset"

    def test_get_dataset_name_from_id_nonexistent(self, in_memory_db_no_connection):
        """Test that getting name of nonexistent dataset ID raises ValueError"""
        with pytest.raises(ValueError, match="Dataset with ID '999' does not exist."):
            db.get_dataset_name_from_id(in_memory_db_no_connection, 999)

    def test_delete_dataset(self, in_memory_db):
        """Test deleting a dataset"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)
        db.delete_dataset(in_memory_db, dataset_id)

        assert db.dataset_exists(in_memory_db, dataset_id, None) is False

    def test_get_all_datasets(self, in_memory_db_no_connection):
        """Test retrieving all datasets"""
        with db.open_database(in_memory_db_no_connection) as conn:
            id1 = db.insert_dataset(conn, "dataset1", None)
            id2 = db.insert_dataset(conn, "dataset2", None)
            conn.commit()

        datasets = db.get_all_datasets(in_memory_db_no_connection)
        dataset_names = [ds['name'] for ds in datasets]

        assert "dataset1" in dataset_names
        assert "dataset2" in dataset_names

    def test_find_dataset_by_path(self, in_memory_db_no_connection):
        """Test finding dataset by original path"""
        dataset_path = "C:/data/test-dataset"
        with db.open_database(in_memory_db_no_connection) as conn:
            dataset_id = db.insert_dataset(conn, "test-dataset", None)
            db.insert_version(
                conn, dataset_id, "hash1", 1.0, dataset_path, None
            )
            conn.commit()

        found_id = db.find_dataset_by_path(in_memory_db_no_connection, dataset_path)
        assert found_id == dataset_id

# --------------------------- OBJECT OPERATIONS --------------------------------

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
        db.insert_object(in_memory_db, "abc123", 2000)

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT COUNT(*) as count, size FROM objects WHERE hash = ?", ("abc123",))
        row = cursor.fetchone()

        assert row['count'] == 1
        assert row['size'] == 1000

    def test_object_is_used(self, dataset_with_version):
        """Test checking if object is referenced by versions"""
        conn = dataset_with_version['conn']
        object_hash = dataset_with_version['object_hash']

        assert db.object_is_used(conn, object_hash) is True
        assert db.object_is_used(conn, "unused_hash") is False

    def test_delete_unused_objects(self, in_memory_db):
        """Test deleting objects not referenced by any files
         - add objects with both used and unused hashes
         - add version + file referencing one of the hashes
         - delete unused objects and verify only the unreferenced one is deleted
        """
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)

        db.insert_object(in_memory_db, "used_hash", 1000)
        db.insert_object(in_memory_db, "unused_hash", 2000)

        version_id = db.insert_version(in_memory_db, dataset_id, "used_hash", 1.0, "/path", None)
        db.insert_files(in_memory_db, version_id, "used_hash", "file.txt")

        deleted = db.delete_object(in_memory_db)

        assert "unused_hash" in deleted
        assert "used_hash" not in deleted

    def test_get_object_size(self, in_memory_db_no_connection):
        """Test retrieving object size by hash"""
        with db.open_database(in_memory_db_no_connection) as conn:
            db.insert_object(conn, "abc123", 1500)
            conn.commit()

        size = db.get_object_size(in_memory_db_no_connection, "abc123")
        assert size == 1500

        size_none = db.get_object_size(in_memory_db_no_connection, "nonexistent_hash")
        assert size_none == 0

# ---------------------- VERSION OPERATIONS ----------------------

class TestVersionOperations:
    """Test version tracking operations"""

    def test_insert_version(self, in_memory_db):
        """Test inserting a new version
         - insert dataset beforehand to satisfy foreign key constraints
        """
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)

        version_id = db.insert_version(
            in_memory_db, dataset_id, "abc123", 1.0, "/path/to/data", "Initial version"
        )

        assert version_id == 1

        cursor = in_memory_db.cursor()
        cursor.execute("SELECT * FROM versions WHERE id = ?", (version_id,))
        version = cursor.fetchone()

        assert version['version'] == 1.0
        assert version['original_path'] == "C:\\path\\to\\data"
        assert version['object_hash'] == "abc123"
        assert version['message'] == "Initial version"

    def test_check_version_exists(self, dataset_with_version):
        """Test checking if a version exists
         - test both existing and non-existing versions
        """
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']

        assert db.check_version_exists(conn, dataset_id, 1.0) is True
        assert db.check_version_exists(conn, dataset_id, 2.0) is False

    def test_get_latest_version_empty_dataset(self, in_memory_db):
        """Test getting latest version for dataset with no versions"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)
        assert db.get_latest_version(in_memory_db, dataset_id) == 0.0

    def test_get_latest_version_with_versions(self, dataset_with_version):
        """Test getting latest version for dataset with multiple versions
         - insert additional versions to test
        """
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']

        db.insert_object(conn, "def456", 2000)
        db.insert_version(conn, dataset_id, "def456", 1.5, "/path2", None)
        db.insert_object(conn, "ghi789", 3000)
        db.insert_version(conn, dataset_id, "ghi789", 2.0, "/path3", None)

        assert db.get_latest_version(conn, dataset_id) == 2.0

    def test_get_first_version_empty_dataset(self, in_memory_db):
        """Test getting first version for dataset with no versions"""
        dataset_id = db.insert_dataset(in_memory_db, "test-dataset", None)
        assert db.get_first_version(in_memory_db, dataset_id) is None

    def test_get_first_version_with_versions(self, dataset_with_version):
        """Test getting first version for dataset with multiple versions
         - insert additional versions to test
        """
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']

        db.insert_object(conn, "def12", 2000)
        db.insert_version(conn, dataset_id, "def12", 1.5, "/path2", None)
        db.insert_object(conn, "ghi789", 3000)
        db.insert_version(conn, dataset_id, "ghi789", 2.0, "/path3", None)

        assert db.get_first_version(conn, dataset_id) == 1.0

    def test_delete_versions(self, dataset_with_version):
        """Test deleting all versions for a dataset"""
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']

        db.delete_versions(conn, dataset_id)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM versions WHERE dataset_id = ?", (dataset_id,))
        assert cursor.fetchone()['count'] == 0

    def test_get_dataset_history(self, dataset_with_version_no_conn):
        """Test retrieving all versions for a specific dataset
         - use fixtures version and create an additional one to test multiple versions
        """
        dataset_id = dataset_with_version_no_conn['dataset_id']
        db_path = dataset_with_version_no_conn['db_path']
        with db.open_database(db_path) as conn:
            db.insert_version(conn, dataset_id, "test_hash", 2.0, "/path2", "Second version")
            conn.commit()
        versions = db.get_dataset_history(db_path, dataset_id, None)

        assert len(versions) == 2
        assert versions[0]['version'] == 1.0
        assert versions[0]['original_path'] == "C:\\path\\to\\data"

        assert versions[1]['version'] == 2.0
        assert versions[1]['original_path'] == "C:\\path2"

    def test_hash_exists(self, dataset_with_version):
        """Test checking if a hash exists in versions table
         - test both existing and non-existing hashes
        """
        conn = dataset_with_version['conn']
        object_hash = dataset_with_version['object_hash']

        version_info =  db.hash_exists(conn, object_hash)
        assert version_info is not None
        assert db.hash_exists(conn, "nonexistent_hash") is None

# ---------------- FILE OPERATIONS ------------------

class TestFileOperations:
    """Test file tracking operations"""

    def test_insert_files(self, dataset_with_version):
        """Test inserting files associated with a version
         - inset objects beforehand to satisfy foreign key constraints
        """
        conn = dataset_with_version['conn']
        version_id = dataset_with_version['version_id']

        db.insert_object(conn, "file_hash", 500)
        db.insert_files(conn, version_id, "file_hash", "relative/path/file.txt")

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM files WHERE version_id = ?", (version_id,))
        file_row = cursor.fetchone()

        assert file_row is not None
        assert file_row['object_hash'] == "file_hash"
        assert file_row['relative_path'] == "relative/path/file.txt"

    def test_delete_files(self, dataset_with_version):
        """Test deleting all files for a dataset
         - should delete files associated with all versions of the dataset
         - have to also create the objects because of foreign key constraints
        """
        conn = dataset_with_version['conn']
        dataset_id = dataset_with_version['dataset_id']
        version_id = dataset_with_version['version_id']

        db.insert_object(conn, "hash1", 100)
        db.insert_object(conn, "hash2", 200)
        db.insert_files(conn, version_id, "hash1", "file1.txt")
        db.insert_files(conn, version_id, "hash2", "file2.txt")

        db.delete_files(conn, dataset_id)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM files WHERE version_id = ?", (version_id,))
        assert cursor.fetchone()['count'] == 0

    def test_get_files_for_version(self, dataset_with_version_no_conn):
        """Test retrieving all files for a specific version
         - insert objects and files beforehand to set up the test
         - for inserting open a connection, but tested function opens its own
        """
        dataset_id = dataset_with_version_no_conn['dataset_id']
        version_id = dataset_with_version_no_conn['version_id']
        db_path = dataset_with_version_no_conn['db_path']

        with db.open_database(db_path) as conn:
            db.insert_object(conn, "hash1", 100)
            db.insert_object(conn, "hash2", 200)
            db.insert_files(conn, version_id, "hash1", "file1.txt")
            db.insert_files(conn, version_id, "hash2", "file2.txt")
            conn.commit()

        files = db.get_files_for_version(db_path, dataset_id, None, 1.0)

        assert len(files) == 2
        relative_paths = [f['relative_path'] for f in files]
        assert "file1.txt" in relative_paths
        assert "file2.txt" in relative_paths
