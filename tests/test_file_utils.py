from data_tracker import file_utils as fu
import tempfile
import shutil
import pytest
import os

def test_format_size():
    """Test the format_size function formats sizes correctly."""
    assert fu.format_size(0) == "0.00 B"
    assert fu.format_size(500) == "500.00 B"
    assert fu.format_size(1024) == "1.00 KB"
    assert fu.format_size(1536) == "1.50 KB"
    assert fu.format_size(1048576) == "1.00 MB"
    assert fu.format_size(1073741824) == "1.00 GB"
    assert fu.format_size(1099511627776) == "1.00 TB"

def test_cleanup_temp_files(monkeypatch):
    """Test that the cleanup_temp_files function removes correct temporary files.
     - create temporary directory and files for testing
     - mock tempfile.gettempdir to return the test directory
     - call cleanup_temp_files and check that only the correct files are removed
    """
    test_temp_dir = tempfile.mkdtemp()
    try:
        monkeypatch.setattr(tempfile, 'gettempdir', lambda: test_temp_dir)

        test_file1 = os.path.join(test_temp_dir, "dt_view_test1.csv")
        test_file2 = os.path.join(test_temp_dir, "dt_view_test2.html")
        other_file = os.path.join(test_temp_dir, "other_file.txt")

        for filepath in [test_file1, test_file2, other_file]:
            with open(filepath, "w") as f:
                f.write("test content")

        removed, failed = fu.cleanup_temp_files()

        assert removed == 2
        assert failed == 0
        assert not os.path.exists(test_file1)
        assert not os.path.exists(test_file2)
        assert os.path.exists(other_file)
    finally:
        import shutil
        shutil.rmtree(test_temp_dir, ignore_errors=True)

def test_cleanup_temp_files_with_errors(monkeypatch):
    """Test that the cleanup_temp_files function handles errors gracefully.
     - create temporary directory and files for testing
     - mock tempfile.gettempdir to return the test directory
     - mock os.remove to raise an exception for one of the files
     - call cleanup_temp_files and check that it counts the error correctly
    """
    test_temp_dir = tempfile.mkdtemp()
    try:
        monkeypatch.setattr(tempfile, 'gettempdir', lambda: test_temp_dir)

        test_file1 = os.path.join(test_temp_dir, "dt_view_test1.csv")
        test_file2 = os.path.join(test_temp_dir, "dt_view_test2.html")
        for filepath in [test_file1, test_file2]:
            with open(filepath, "w") as f:
                f.write("test content")

        original_remove = os.remove

        def mock_remove(path):
            if path == test_file1:
                raise OSError("Mocked error")
            else:
                original_remove(path)

        monkeypatch.setattr(os, 'unlink', mock_remove)

        removed, failed = fu.cleanup_temp_files()

        assert removed == 1
        assert failed == 1
        assert os.path.exists(test_file1)
        assert not os.path.exists(test_file2)
    finally:
        shutil.rmtree(test_temp_dir, ignore_errors=True)

def test_export_file_invalid_dataset():
    """Test that export_file returns an error for invalid export paths."""
    with pytest.raises(ValueError, match="Dataset with name 'nonexistent' does not exist"):
        success, message = fu.export_file("/invalid/path/dataset.csv", None,
                                          "nonexistent", 1.0, False, False)

