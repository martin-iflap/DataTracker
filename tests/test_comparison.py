from data_tracker import comparison as comp
from data_tracker import db_manager as db
import shutil


def test_compare_with_invalid_version(tmp_path):
    """Test comparison fails gracefully with invalid version number"""
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