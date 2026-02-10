from data_tracker import core


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