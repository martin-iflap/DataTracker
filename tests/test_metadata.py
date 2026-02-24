from data_tracker import db_manager as db
from data_tracker import metadata


class TestRenameDataset:
    """Test cases for the rename_dataset function in metadata.py"""

    def test_rename_dataset(self, temp_tracker_dir):
        """Test renaming a dataset
         - Create a dataset in the database
         - Call rename_dataset to change its name
         - Verify that the name was updated in the database
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Old Name", "Initial message")
            conn.commit()

            success, msg = metadata.rename_dataset(dataset_id, "Old Name", "New Name")
            assert success, f"Rename failed: {msg}"

            cursor = conn.execute("SELECT name FROM datasets WHERE id = ?", (dataset_id,))
            row = cursor.fetchone()
            assert row is not None, "Dataset not found after rename"
            assert row[0] == "New Name", f"Dataset name was not updated, expected 'New Name' but got '{row[0]}'"

    def test_rename_dataset_to_existing_name(self, temp_tracker_dir):
        """Test renaming a dataset to a name that already exists
         - Create two datasets in the database
         - Attempt to rename one dataset to the name of the other
         - Verify that the rename fails with an appropriate error message
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            id1 = db.insert_dataset(conn, "Dataset One", "Message one")
            id2 = db.insert_dataset(conn, "Dataset Two", "Message two")
            conn.commit()

            success, msg = metadata.rename_dataset(id1, "Dataset One", "Dataset Two")
            assert not success, "Rename should have failed due to duplicate name"
            assert "already exists" in msg, f"Unexpected error message: {msg}"

    def test_rename_nonexistent_dataset(self, temp_tracker_dir):
        """Test renaming a dataset that does not exist
         - Attempt to rename a dataset with an ID and name that do not exist in the database
         - Verify that the rename fails with an appropriate error message
        """
        success, msg = metadata.rename_dataset(9999, "Nonexistent", "New Name")
        assert not success, "Rename should have failed for nonexistent dataset"
        assert "does not exist" in msg, f"Unexpected error message: {msg}"

    def test_rename_with_same_name(self, temp_tracker_dir):
        """Test renaming a dataset to the same name
         - Create a dataset in the database
         - Attempt to rename it to the same name
         - Verify that the rename succeeds and does not cause any issues
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Same Name", "Message")
            conn.commit()

            success, msg = metadata.rename_dataset(dataset_id, "Same Name", "Same Name")
            assert success, f"Rename failed when using the same name: {msg}"
            assert "Dataset name is already 'Same Name'" in msg, f"Unexpected message when renaming to the same name: {msg}"

            cursor = conn.execute("SELECT name FROM datasets WHERE id = ?", (dataset_id,))
            row = cursor.fetchone()
            assert row is not None, "Dataset not found after rename"
            assert row[0] == "Same Name", f"Dataset name was changed unexpectedly, expected 'Same Name' but got '{row[0]}'"

    def test_rename_dataset_with_dataset_name(self, temp_tracker_dir):
        """Test renaming a dataset by providing None for the ID and using the name to identify it
         - Create a dataset in the database
         - Call rename_dataset with None for the ID and the old name to identify the dataset
         - Verify that the name was updated in the database
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Old Name", "Initial message")
            conn.commit()

            success, msg = metadata.rename_dataset(None, "Old Name", "New Name")
            assert success, f"Rename failed: {msg}"

            cursor = conn.execute("SELECT name FROM datasets WHERE id = ?", (dataset_id,))
            row = cursor.fetchone()
            assert row is not None, "Dataset not found after rename"
            assert row[0] == "New Name", f"Dataset name was not updated, expected 'New Name' but got '{row[0]}'"

    def test_rename_dataset_with_dataset_name_nonexistent_name(self, temp_tracker_dir):
        """Test renaming a dataset by providing None for the ID and a name that does not exist
         - Attempt to rename a dataset by providing None for the ID and a name that does not exist in the database
         - Verify that the rename fails with an appropriate error message
        """
        success, msg = metadata.rename_dataset(None, "Nonexistent Name", "New Name")
        assert not success, "Rename should have failed for nonexistent name when ID is None"
        assert "does not exist" in msg, f"Unexpected error message: {msg}"

    def test_rename_dataset_with_dataset_name_duplicate_name(self, temp_tracker_dir):
        """Test renaming a dataset by providing None for the ID and a new name that already exists
         - Create two datasets in the database
         - Attempt to rename one dataset by providing None for the ID and a new name that already exists in the database
         - Verify that the rename fails with an appropriate error message
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            id1 = db.insert_dataset(conn, "Dataset One", "Message one")
            id2 = db.insert_dataset(conn, "Dataset Two", "Message two")
            conn.commit()

            success, msg = metadata.rename_dataset(None, "Dataset One", "Dataset Two")
            assert not success, "Rename should have failed due to duplicate name when ID is None"
            assert "already exists" in msg, f"Unexpected error message: {msg}"

    def test_rename_tracker_not_initialized(self, monkeypatch):
        """Test renaming a dataset when the data tracker is not initialized
         - Mock find_data_tracker_root to return None to simulate uninitialized tracker
         - Attempt to rename a dataset and verify that it fails with an appropriate error message
        """
        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root', lambda: None)

        success, msg = metadata.rename_dataset(1, "Old Name", "New Name")
        assert not success, "Rename should have failed when data tracker is not initialized"
        assert "Data tracker is not initialized. Please run 'dt init' first." in msg, f"Unexpected error message: {msg}"

    def test_rename_dataset_get_id_returns_none(self, temp_tracker_dir, monkeypatch):
        """Test renaming when get_id_from_name returns None (defensive check for edge case)
         - Create a dataset
         - Mock get_id_from_name to return None to test the defensive None check on line 28-29
         - Verify proper error handling
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            db.insert_dataset(conn, "Test Dataset", "Message")
            conn.commit()

        monkeypatch.setattr('data_tracker.db_manager.get_id_from_name', lambda connection, name: None)

        success, msg = metadata.rename_dataset(None, "Test Dataset", "New Name")
        assert not success, "Rename should have failed when get_id_from_name returns None"
        assert "Dataset with name 'Test Dataset' does not exist." in msg, f"Unexpected error message: {msg}"

# ------------- change_message tests --------------

class TestChangeMessage:
    """Test cases for the change_message function in metadata.py"""

    def test_change_message(self, temp_tracker_dir):
        """Test changing the message of a dataset
         - Create a dataset in the database
         - Call change_message to update its message
         - Verify that the message was updated in the database
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Dataset Name", "Old message")
            conn.commit()

            success, msg = metadata.change_message("New message", dataset_id, "Dataset Name", dataset=True)
            assert success, f"Change message failed: {msg}"

            cursor = conn.execute("SELECT message FROM datasets WHERE id = ?", (dataset_id,))
            row = cursor.fetchone()
            assert row is not None, "Dataset not found after changing message"
            assert row[0] == "New message", f"Dataset message was not updated, expected 'New message' but got '{row[0]}'"

    def test_change_message_nonexistent_dataset(self, temp_tracker_dir):
        """Test changing the message of a dataset that does not exist
         - Attempt to change the message of a dataset with an ID and name that do not exist in the database
         - Verify that the change fails with an appropriate error message
        """
        success, msg = metadata.change_message("New message", 9999, "Nonexistent", dataset=True)
        assert not success, "Change message should have failed for nonexistent dataset"
        assert "does not exist" in msg, f"Unexpected error message: {msg}"

    def test_change_message_with_dataset_name(self, temp_tracker_dir):
        """Test changing the message of a dataset by providing None for the ID and using the name to identify it
         - Create a dataset in the database
         - Call change_message with None for the ID and the name to identify the dataset
         - Verify that the message was updated in the database
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Dataset Name", "Old message")
            conn.commit()

            success, msg = metadata.change_message("New message", None, "Dataset Name", dataset=True)
            assert success, f"Change message failed: {msg}"

            cursor = conn.execute("SELECT message FROM datasets WHERE id = ?", (dataset_id,))
            row = cursor.fetchone()
            assert row is not None, "Dataset not found after changing message"
            assert row[0] == "New message", f"Dataset message was not updated, expected 'New message' but got '{row[0]}'"

    def test_change_message_with_dataset_name_nonexistent_name(self, temp_tracker_dir):
        """Test changing the message of a dataset by providing None for the ID and a name that does not exist
         - Attempt to change the message of a dataset by providing None for the ID and a name that does not exist in the database
         - Verify that the change fails with an appropriate error message
        """
        success, msg = metadata.change_message("New message", None, "Nonexistent Name", dataset=True)
        assert not success, "Change message should have failed for nonexistent name when ID is None"
        assert "does not exist" in msg, f"Unexpected error message: {msg}"

    def test_change_message_tracker_not_initialized(self, monkeypatch):
        """Test changing the message of a dataset when the data tracker is not initialized
         - Mock find_data_tracker_root to return None to simulate uninitialized tracker
         - Attempt to change the message of a dataset and verify that it fails with an appropriate error message
        """
        monkeypatch.setattr('data_tracker.file_utils.find_data_tracker_root', lambda: None)

        success, msg = metadata.change_message("New message", 1, "Dataset Name", dataset=True)
        assert not success, "Change message should have failed when data tracker is not initialized"
        assert "Data tracker is not initialized. Please run 'dt init' first." in msg, f"Unexpected error message: {msg}"

    def test_change_message_no_versions_found(self, temp_tracker_dir):
        """Test changing the message of a version when no versions exist for the dataset
         - Create a dataset in the database without any versions
         - Attempt to change the message of the latest version (which would be 0.0)
         - Verify that it fails because no version 0.0 exists to update
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Dataset Name", "Old message")
            conn.commit()

            success, msg = metadata.change_message("New message", dataset_id, "Dataset Name", provided_version="latest", dataset=False)
            assert not success, "Change message should have failed when no versions exist for the dataset"
            assert "No dataset found with ID: 1 and version: 0.0 to update the message." in msg, f"Unexpected error message: {msg}"

    def test_change_message_latest_version(self, temp_tracker_dir):
        """Test changing the message of the latest version of a dataset
         - Create a dataset in the database and add multiple versions
         - Call change_message with provided_version set to 'latest' to update the message of the latest version
         - Verify that the message of the latest version was updated in the database
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Dataset Name", "Old message")
            version1 = db.insert_version(conn, dataset_id, "hash1", 1, "path1", "Version 1 message")
            version2 = db.insert_version(conn, dataset_id, "hash2", 2, "path2", "Version 2 message")
            conn.commit()

            success, msg = metadata.change_message("New message for latest version", dataset_id, "Dataset Name", provided_version="latest", dataset=False)
            assert success, f"Change message failed: {msg}"

            cursor = conn.execute("SELECT message FROM versions WHERE dataset_id = ? AND version = ?", (dataset_id, version2))
            row = cursor.fetchone()
            assert row is not None, "Version not found after changing message"
            assert row[0] == "New message for latest version", f"Version message was not updated, expected 'New message for latest version' but got '{row[0]}'"

    def test_change_message_specific_version(self, temp_tracker_dir):
        """Test changing the message of a specific version of a dataset
         - Create a dataset in the database and add multiple versions
         - Call change_message with a specific version number to update the message of that version
         - Verify that the message of the specified version was updated in the database
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Dataset Name", "Old message")
            version1 = db.insert_version(conn, dataset_id, "hash1", 1, "path1", "Version 1 message")
            version2 = db.insert_version(conn, dataset_id, "hash2", 2, "path2", "Version 2 message")
            conn.commit()

            success, msg = metadata.change_message("New message for version 1", dataset_id, "Dataset Name", provided_version=1, dataset=False)
            assert success, f"Change message failed: {msg}"

            cursor = conn.execute("SELECT message FROM versions WHERE dataset_id = ? AND version = ?", (dataset_id, version1))
            row = cursor.fetchone()
            assert row is not None, "Version not found after changing message"
            assert row[0] == "New message for version 1", f"Version message was not updated, expected 'New message for version 1' but got '{row[0]}'"

    def test_change_message_dataset(self, temp_tracker_dir):
        """Test changing the message of a dataset by setting dataset=True
         - Create a dataset in the database
         - Call change_message with dataset=True to update the message of the dataset
         - Verify that the message of the dataset was updated in the database
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Dataset Name", "Old message")
            conn.commit()

            success, msg = metadata.change_message("New message for dataset", dataset_id, "Dataset Name", dataset=True)
            assert success, f"Change message failed: {msg}"

            cursor = conn.execute("SELECT message FROM datasets WHERE id = ?", (dataset_id,))
            row = cursor.fetchone()
            assert row is not None, "Dataset not found after changing message"
            assert row[0] == "New message for dataset", f"Dataset message was not updated, expected 'New message for dataset' but got '{row[0]}'"

    def test_change_message_identical_dataset_id(self, monkeypatch):
        """Test changing the message of a dataset when multiple datasets have the same ID (should not happen but test for robustness)
         - monkeypatch the db.update_dataset_message function to return 2 to simulate multiple rows (datasets) being updated
         - Attempt to change the message of the dataset by ID and verify that it warns correctly
         """
        monkeypatch.setattr("data_tracker.db_manager.update_dataset_message", lambda conn, id, new_message: 2)

        success, msg = metadata.change_message("New message for dataset with duplicate ID", 1, None, dataset=True)
        assert not success, "Change message should have failed due to multiple datasets with the same ID"
        assert "Multiple datasets found with ID: 1" in msg, f"Unexpected error message: {msg}"

    def test_change_message_get_id_returns_none(self, temp_tracker_dir, monkeypatch):
        """Test change_message when get_id_from_name returns None (defensive check for edge case)
         - Create a dataset
         - Mock get_id_from_name to return None to test the defensive None check on line 56-57
         - Verify proper error handling
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            db.insert_dataset(conn, "Test Dataset", "Message")
            conn.commit()

        monkeypatch.setattr('data_tracker.db_manager.get_id_from_name', lambda connection, name: None)

        success, msg = metadata.change_message("New message", None, "Test Dataset", dataset=True)
        assert not success, "Change message should have failed when get_id_from_name returns None"
        assert "Dataset with name 'Test Dataset' does not exist." in msg, f"Unexpected error message: {msg}"

    def test_change_message_get_latest_version_returns_none(self, temp_tracker_dir, monkeypatch):
        """Test change_message when get_latest_version returns None (defensive check for edge case)
         - Create a dataset
         - Mock get_latest_version to return None to test the defensive None check on line 61-62
         - Verify proper error handling
        """
        db_path = temp_tracker_dir['db_path']
        with db.open_database(db_path) as conn:
            dataset_id = db.insert_dataset(conn, "Test Dataset", "Message")
            conn.commit()

        monkeypatch.setattr('data_tracker.db_manager.get_latest_version', lambda connection, id: None)

        success, msg = metadata.change_message("New message", dataset_id, "Test Dataset", provided_version="latest", dataset=False)
        assert not success, "Change message should have failed when get_latest_version returns None"
        assert f"No versions found for dataset with ID: {dataset_id}." in msg, f"Unexpected error message: {msg}"
