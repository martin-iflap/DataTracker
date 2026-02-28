# DataTracker

A lightweight version control system for data files with a git-like CLI interface.

[![Python 3.14+](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Track dataset versions, compare changes, and transform data using Docker containers. Built for data scientists and ML engineers who need reproducible data pipelines.

## Features

- Git-like interface with familiar commands
- Content-addressable storage with automatic deduplication
- Track multiple dataset versions with metadata
- Docker integration for reproducible transformations
- Compare versions with text diffs and binary similarity metrics
- Export any version to any location
- Monitor storage usage across datasets

## Installation

**Prerequisites:** Python 3.14+, Docker (optional, for `transform` command)

```bash
git clone https://github.com/martin-iflap/DataTracker.git
cd DataTracker
pip install -e .
dt --help
```

## Quick Start

```bash
# Initialize a tracker
dt init

# Add a dataset
dt add ./data/sales.csv --title "sales-data" -m "Initial sales data"

# Update with a new version
dt update ./data/sales_cleaned.csv --name sales-data -m "Cleaned missing values"

# List all tracked datasets
dt ls

# View history
dt history --name sales-data

# Compare versions
dt compare 1.0 2.0 --name sales-data

# Export a specific version
dt export ./output --name sales-data -v 1.0
```

## Command Reference

### Core Commands

#### `dt init`
Initialize a new DataTracker repository in the current directory.

```bash
dt init
```

Creates a `.data_tracker/` directory with:
- `tracker.db` - SQLite database for metadata
- `objects/` - Content-addressable storage for file data


#### `dt add`
Add a new dataset (file or directory) to tracking.

```bash
dt add <path> [OPTIONS]

Options:
  --title TEXT        Custom name for the dataset (default: auto-generated)
  -v, --version FLOAT Version number (default: 1.0)
  -m, --message TEXT  Descriptive message
```

**Examples:**
```bash
# Add a single file
dt add ./data.csv --title "experiment-results" -m "Initial experiment"

# Add a directory
dt add ./dataset/ --title "image-collection"

# Add with custom version
dt add ./model.pkl --title "model-v2" -v 2.5 -m "Updated hyperparameters"
```


#### `dt update`
Add a new version of an existing dataset.

```bash
dt update <path> [OPTIONS]

Options:
  --id INT            Dataset ID
  --name TEXT         Dataset name
  -v, --version FLOAT Custom version (auto-increments if not specified)
  -m, --message TEXT  Description of changes

Note: Provide exactly one of --id or --name
```

**Examples:**
```bash
# Update by name
dt update ./data_v2.csv --name sales-data -m "Added Q4 data"

# Update by ID with custom version
dt update ./data_v3.csv --id 1 -v 3.0 -m "Major update"
```


#### `dt ls`
List all tracked datasets.

```bash
dt ls [OPTIONS]

Options:
  -s, --structure    Show file structure for each dataset
```

**Examples:**
```bash
# Simple list
dt ls

# With file structure
dt ls --structure
```


#### `dt history`
Show version history for a dataset.

```bash
dt history [OPTIONS]

Options:
  --id INT            Dataset ID
  --name TEXT         Dataset name
  -d, --detailed      Show detailed file changes

Note: Provide exactly one of --id or --name
```

**Examples:**
```bash
# View history
dt history --name sales-data

# Detailed history with file changes
dt history --id 1 -d
```


#### `dt remove`
Remove a dataset and all its versions from tracking.

```bash
dt remove [OPTIONS]

Options:
  --id INT      Dataset ID
  --name TEXT   Dataset name

Note: Provide exactly one of --id or --name
```

**Examples:**
```bash
dt remove --name old-experiment
dt remove --id 5
```

#### `dt rename`
Rename a dataset.

```bash
dt rename <new_name> [OPTIONS]

Options:
  --id INT      Dataset ID
  -n, --name    Current name of the dataset

Note: Provide exactly one of --id or --name
```

**Examples:**
```bash
dt rename "updated-sales-data" --name sales-data
dt rename "experiment-final" --id 5
```

#### `dt annotate`
Update the message for a dataset or specific version.

```bash
dt annotate <new_message> [OPTIONS]

Options:
  --id INT            Dataset ID
  -n, --name TEXT     Dataset name
  -v, --version FLOAT Specific version number
  --latest            Annotate the most recent version
  --dataset           Annotate the dataset itself (not a version)

Note: Provide exactly one of --id or --name
Note: Provide exactly one of --version, --latest, or --dataset
```

**Examples:**
```bash
# Annotate specific version
dt annotate "Fixed outliers" --id 5 --version 1.0

# Annotate latest version
dt annotate "Production ready" --name mydata --latest

# Annotate dataset message
dt annotate "Customer churn dataset" --id 5 --dataset
```

### Advanced Commands

#### `dt view`
Open a specific version of a dataset in the system's default application.

```bash
dt view -v <version> [OPTIONS]

Options:
  -v, --version FLOAT  Version to view (required)
  --id INT             Dataset ID
  --name TEXT          Dataset name

Note: Provide exactly one of --id or --name
```

**Examples:**
```bash
# View version 1.0
dt view -v 1.0 --name sales-data

# View older version
dt view -v 2.5 --id 3
```


#### `dt compare`
Compare two versions of a dataset and show differences.

```bash
dt compare <v1> <v2> [OPTIONS]
If versions are not specified, dt compares the first and the latest versions.

Options:
  --id INT      Dataset ID
  --name TEXT   Dataset name

Note: Provide exactly one of --id or --name
```

**Examples:**
```bash
# Compare versions
dt compare 1.0 2.0 --name sales-data

# Compare non-sequential versions
dt compare 1.0 3.5 --id 1
```

**Features:**
- Text file diffs with colorized output
- Binary file similarity metrics
- File structure comparison (added/removed/modified files)


#### `dt export`
Export a specific version to a directory.

```bash
dt export <export_path> [OPTIONS]

Options:
  -v, --version FLOAT      Version to export (required)
  --id INT                 Dataset ID
  --name TEXT              Dataset name
  -f, --force              Overwrite existing files
  -r, --preserve-root      Keep root directory name

Note: Provide exactly one of --id or --name
```

**Examples:**
```bash
# Export to a directory
dt export ./output --name sales-data -v 2.0

# Export with overwrite
dt export ./backup --id 1 -v 1.0 --force

# Preserve original directory structure
dt export ./restore --name dataset -v 3.0 --preserve-root
```


#### `dt transform`
Transform data using a Docker container with automatic versioning.

```bash
dt transform [OPTIONS]

Options:
  -i, --image TEXT         Docker image to use (required)
  -in, --input-data TEXT   Input data path (required)
  -out, --output-data TEXT Output data path (required)
  -c, --command TEXT       Shell command to run in container (required)
  -f, --force              Skip command validation
  --auto-track             Auto-add input if not tracked
  --no-track               Skip versioning output
  -id, --dataset-id INT    Explicitly specify dataset ID
  -v, --version FLOAT      Manual version number
  -m, --message TEXT       Version message
```

**Examples:**
```bash
# Sort CSV data
dt transform \
  --image alpine:latest \
  --input-data ./data.csv \
  --output-data ./sorted/ \
  --command "sort /input/*.csv > /output/sorted.csv"

# Process with Python container
dt transform \
  -i python:3.11-slim \
  -in ./raw_data/ \
  -out ./processed/ \
  -c "python /input/process.py --output /output/" \
  -m "Applied normalization"

# Auto-track new dataset
dt transform \
  -i busybox:latest \
  -in ./untracked_data/ \
  -out ./result/ \
  -c "cat /input/*.txt | wc -l > /output/count.txt" \
  --auto-track
```

**Key Features:**
- Automatic version creation for tracked datasets
- Input validation (checks for `/input` and `/output` references)
- Rollback on failure (removes auto-added datasets)
- Detailed error messages for Docker issues

**Mount Paths:**
Your command MUST reference:
- `/input` - Read-only input data
- `/output` - Write output results here


#### `dt storage`
Display storage statistics for tracked datasets.

```bash
dt storage
```

Shows:
- Total number of tracked files
- Total storage used

## Architecture

### Storage Model

DataTracker uses a **content-addressable storage** system:

```
.data_tracker/
├── tracker.db          # SQLite database (metadata)
└── objects/            # Hash-based file storage
    ├── abc123...       # File content stored by SHA-256 hash
    ├── def456...
    └── ...
```

**Benefits:**
- Automatic deduplication (identical files stored once)
- Integrity verification via hashes
- Efficient storage for large datasets

### Database Schema

**datasets** - Dataset metadata
- `id`, `name`, `message`, `created_at`

**objects** - File content storage
- `hash`, `size`

**versions** - Version metadata
- `id`, `dataset_id`, `object_hash`, `version`, `original_path`, `message`, `created_at`

**files** - File structure within versions
- `id`, `version_id`, `object_hash`, `relative_path`

## Use Cases

### Data Science Workflows
```bash
# Track experiment data
dt add ./raw_data/ --title "experiment-1" -m "Raw sensor data"

# Version preprocessing steps
dt update ./cleaned_data/ --name experiment-1 -m "Removed outliers"

# Compare preprocessing results
dt compare 1.0 2.0 --name experiment-1

# Export for model training
dt export ./training_data --name experiment-1 -v 2.0
```

### Machine Learning Pipeline
```bash
# Version training data
dt add ./train.csv --title "training-set"

# Track augmented data
dt transform \
  -i python:3.11 \
  -in ./train.csv \
  -out ./augmented/ \
  -c "python /input/augment.py > /output/augmented.csv" \
  -m "Applied data augmentation"

# Review changes
dt history --name training-set -d
```

### Data Cleaning Pipeline
```bash
# Initial data
dt add ./raw_sales.csv --title "sales"

# Automated cleaning
dt transform \
  -i pandas:latest \
  -in ./raw_sales.csv \
  -out ./cleaned/ \
  -c "python /input/clean.py /input/*.csv /output/cleaned.csv" \
  -m "Cleaned missing values"

# Compare before/after
dt compare 1.0 2.0 --name sales
```

## Development

### Project Structure

```
DataTracker/
├── src/
│   └── data_tracker/
│       ├── cli.py               # CLI entry point
│       ├── commands.py          # Command implementations
│       ├── core.py              # Core functionality
│       ├── metadata.py          # Rename and annotation operations
│       ├── transform.py         # Docker transformation logic
│       ├── transform_preset.py  # Presets for common transformations
│       ├── comparison.py        # Version comparison
│       ├── db_manager.py        # Database operations
│       ├── docker_manager.py    # Docker integration
│       └── file_utils.py        # File operations
├── tests/                       # Test suite
├── pyproject.toml             # Project configuration
└── README.md
```

### Running Tests

```bash
# Install test dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run with coverage
pytest --cov=data_tracker --cov-report=html

# Run specific test file
pytest tests/test_db_manager.py -v
```

### Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for new functionality
4. Ensure all tests pass (`pytest`)
5. Submit a pull request

## Troubleshooting

### Common Issues

**"Data tracker is not initialized"**
```bash
# Initialize in current directory
dt init
```

**"Docker is not installed"**
- Install Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Verify: `docker --version`

**"Dataset with name 'X' does not exist"**
```bash
# List all datasets to find correct name
dt ls

# Use --id instead of --name
dt history --id 1
```

**"Permission denied" during transform**
- On Linux/Mac: Check file permissions
- On Windows: Ensure Docker Desktop has proper permissions
- Try different output directory

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with:
- [Click](https://click.palletsprojects.com/) - CLI framework
- [SQLite](https://www.sqlite.org/) - Database
- [Colorama](https://github.com/tartley/colorama) - Terminal colors
- [Docker](https://www.docker.com/) - Containerization

