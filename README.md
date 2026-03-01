# DataTracker

A lightweight version control system for data files with a git-like CLI interface.

[![Python 3.14+](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Track dataset versions, compare changes, and transform data using Docker containers. Built for data scientists and engineers who need reproducible, auditable data pipelines.

## Features

- Git-like CLI interface with familiar commands
- Content-addressable storage with automatic deduplication
- Track multiple dataset versions with metadata
- Docker integration for reproducible transformations
- Transform presets for saving and reusing common transformation configurations
- Compare versions with text diffs and binary similarity metrics
- Export any version to any location
- Monitor storage usage across datasets

## Installation

**Prerequisites:** Python 3.14+, Docker (optional — required only for `dt transform`)

```bash
git clone https://github.com/martin-iflap/DataTracker.git
cd DataTracker
pip install -e .
dt --help
```

## Quick Start

```bash
# Initialize a tracker in the current directory
dt init

# Add a dataset
dt add ./data/sales.csv --title "sales-data" -m "Initial sales data"

# Add a new version
dt update ./data/sales_v2.csv --name sales-data -m "Cleaned missing values"

# List all tracked datasets
dt ls

# View version history
dt history --name sales-data

# Compare two versions
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

Creates a `.data_tracker/` directory containing:
- `tracker.db` — SQLite database for all metadata
- `objects/` — Content-addressable file storage (SHA-256 hashed)
- `presets_config.json` — Transform preset configuration


#### `dt add`
Add a new dataset (file or directory) to tracking.

```bash
dt add <path> [OPTIONS]

Options:
  --title TEXT        Name for the dataset (auto-generated if not provided)
  -v, --version FLOAT Version number (default: 1.0)
  -m, --message TEXT  Descriptive message
```

**Examples:**
```bash
# Add a single file
dt add ./data.csv --title "experiment-results" -m "Initial experiment"

# Add a directory
dt add ./dataset/ --title "image-collection" -m "Raw images"

# Add with a custom starting version
dt add ./model.pkl --title "model-v2" -v 2.5 -m "Updated hyperparameters"
```


#### `dt update`
Add a new version to an existing tracked dataset.

```bash
dt update <path> [OPTIONS]

Options:
  --id INT            Dataset ID
  --name TEXT         Dataset name
  -v, --version FLOAT Version number (auto-increments by 1 if not specified)
  -m, --message TEXT  Description of changes

Note: Provide exactly one of --id or --name.
```

**Examples:**
```bash
# Update by name
dt update ./data_v2.csv --name sales-data -m "Added Q4 data"

# Update by ID with an explicit version number
dt update ./data_v3.csv --id 1 -v 3.0 -m "Major restructure"
```


#### `dt ls`
List all tracked datasets.

```bash
dt ls [OPTIONS]

Options:
  -s, --structure    Show the file structure for each dataset's latest version
```

**Examples:**
```bash
dt ls
dt ls --structure
```


#### `dt history`
Show the version history of a dataset.

```bash
dt history [OPTIONS]

Options:
  --id INT       Dataset ID
  --name TEXT    Dataset name
  -d, --detailed Show full details per version: original path, object hash

Note: Provide exactly one of --id or --name.
```

**Examples:**
```bash
dt history --name sales-data
dt history --id 1 --detailed
```


#### `dt remove`
Remove a dataset and all its versions from tracking. Also deletes the associated object files from storage.

```bash
dt remove [OPTIONS]

Options:
  --id INT      Dataset ID
  --name TEXT   Dataset name

Note: Provide exactly one of --id or --name.
```

**Examples:**
```bash
dt remove --name old-experiment
dt remove --id 5
```


#### `dt rename`
Rename a tracked dataset.

```bash
dt rename <new_name> [OPTIONS]

Options:
  --id INT       Dataset ID
  -n, --name     Current name of the dataset

Note: Provide exactly one of --id or --name.
```

**Examples:**
```bash
dt rename "updated-sales-data" --name sales-data
dt rename "experiment-final" --id 5
```


#### `dt annotate`
Update the message for a dataset or a specific version.

```bash
dt annotate <new_message> [OPTIONS]

Options:
  --id INT            Dataset ID
  -n, --name TEXT     Dataset name
  -v, --version FLOAT Target a specific version number
  --latest            Target the most recent version
  --dataset           Target the dataset-level message (not a version)

Note: Provide exactly one of --id or --name.
Note: Provide exactly one of --version, --latest, or --dataset.
```

**Examples:**
```bash
# Update the message for a specific version
dt annotate "Fixed outliers" --id 5 --version 1.0

# Update the message for the latest version
dt annotate "Production ready" --name mydata --latest

# Update the dataset-level message
dt annotate "Customer churn dataset" --id 5 --dataset
```


### Advanced Commands

#### `dt view`
Open a specific version of a dataset in the system's default application.
Single files are opened directly; multi-file versions are reconstructed into a temporary directory and opened there (all previous remaining temp files get deleted when running 'dt view').

```bash
dt view [OPTIONS]

Options:
  -v, --version FLOAT  Version to open (required)
  --id INT             Dataset ID
  --name TEXT          Dataset name

Note: Provide exactly one of --id or --name.
```

**Examples:**
```bash
dt view -v 1.0 --name sales-data
dt view -v 2.5 --id 3
```


#### `dt compare`
Compare two versions of a dataset. If no versions are specified, the two most recent versions are compared automatically.

```bash
dt compare [v1] [v2] [OPTIONS]

Options:
  --id INT      Dataset ID
  --name TEXT   Dataset name

Note: Provide exactly one of --id or --name.
```

**Examples:**
```bash
# Compare two specific versions
dt compare 1.0 2.0 --name sales-data

# Auto-compare the two most recent versions
dt compare --id 1
```

**Output includes:**
- File structure for each version
- Added, removed, and modified files with sizes
- Text diff similarity percentage and line counts for modified files
- Binary similarity percentage for non-text files


#### `dt export`
Export a specific version of a dataset to a given path.

```bash
dt export <export_path> [OPTIONS]

Options:
  -v, --version FLOAT  Version to export (required)
  --id INT             Dataset ID
  --name TEXT          Dataset name
  -f, --force          Overwrite files if they already exist at the destination
  -r, --preserve-root  Recreate the original root directory name at the destination

Note: Provide exactly one of --id or --name.
```

**Examples:**
```bash
# Export to a directory
dt export ./output --name sales-data -v 2.0

# Export and overwrite existing files
dt export ./backup --id 1 -v 1.0 --force

# Export and preserve the original root directory name
dt export ./restore --name dataset -v 3.0 --preserve-root
```


#### `dt transform`
Run a containerized transformation on a tracked dataset using Docker, with automatic versioning of the output.

```bash
dt transform --input-data <path> --output-data <path> [OPTIONS]

Options:
  --input-data TEXT    Path to the input data (required)
  --output-data TEXT   Path to write the output data (required)
  -p, --preset TEXT    Use a saved transform preset (see Transform Presets below)
  -i, --image TEXT     Docker image to use (required if not using a preset)
  -c, --command TEXT   Shell command to run inside the container (required if not using a preset)
  -f, --force          Skip the /input and /output reference check in the command
  --auto-track         Auto-add the input as a new dataset if it is not already tracked
  --no-track           Run the transform without versioning the output
  -id, --dataset-id INT Explicitly specify which dataset ID to version the output under
  -v, --version FLOAT  Manually specify the version number for the output
  -m, --message TEXT   Message for the auto-created output version
```

**How versioning works:**
- If the input path matches a tracked dataset, the output is automatically added as a new version of that dataset.
- If the input is not tracked and `--auto-track` is set, DataTracker first adds the input as a new dataset, then versions the output.
- If the input is not tracked and `--auto-track` is not set, the transform runs but the output is not versioned.
- Use `--no-track` to always skip versioning regardless of tracking status.

**Mount paths:**
Your command must reference the container's mount points:
- `/input` — read-only mount of your input data
- `/output` — write your results here

**Examples:**
```bash
# Sort a CSV using a lightweight Alpine container
dt transform \
  --input-data ./data.csv \
  --output-data ./sorted/ \
  --image alpine:latest \
  --command "sort /input/data.csv > /output/sorted.csv"

# Process with a Python container
dt transform \
  --input-data ./raw_data/ \
  --output-data ./processed/ \
  --image python:3.11-slim \
  --command "python /input/process.py --output /output/" \
  --message "Applied normalisation"

# Auto-track an untracked input and version the output
dt transform \
  --input-data ./untracked_data/ \
  --output-data ./result/ \
  --image busybox:latest \
  --command "cat /input/*.txt | wc -l > /output/count.txt" \
  --auto-track

# Use a saved preset
dt transform \
  --input-data ./raw_data/ \
  --output-data ./processed/ \
  --preset my-python-pipeline
```


#### `dt storage`
Display storage statistics for the current tracker.

```bash
dt storage
```

Shows the total number of stored object files and their combined size on disk.


### Transform Presets

Transform presets let you save a transformation configuration — image, command, flags, and message — and reuse it by name instead of repeating all options on every run. Presets are stored in `.data_tracker/presets_config.json`, which is created automatically when you run `dt init`.

**Preset configuration format:**

```json
{
    "presets": {
        "my-preset-name": {
            "image": "python:3.11-slim",
            "command": "python /input/script.py --output /output/result.csv",
            "auto_track": false,
            "no_track": false,
            "force": false,
            "message": "Ran my pipeline"
        }
    },
    "schema_version": "1.0"
}
```

**Supported preset fields:**

| Field | Type | Description |
|---|---|---|
| `image` | string | Docker image to use |
| `command` | string | Shell command to run inside the container |
| `auto_track` | boolean | Equivalent to `--auto-track` |
| `no_track` | boolean | Equivalent to `--no-track` |
| `force` | boolean | Equivalent to `--force` |
| `message` | string | Default version message |

**Override behavior:** Any option explicitly passed on the CLI takes precedence over the preset value. `--input-data` and `--output-data` are always required on the CLI and are never stored in a preset.

**Using a preset:**

```bash
dt transform \
  --input-data ./raw/ \
  --output-data ./processed/ \
  --preset my-preset-name

# Override a single preset field at runtime
dt transform \
  --input-data ./raw/ \
  --output-data ./processed/ \
  --preset my-preset-name \
  --message "Override message for this run"
```

> **Note:** Preset management commands (`add`, `remove`, `list`) are planned for a future release. For now, presets are managed by editing `presets_config.json` directly.


## Architecture

### Storage Model

DataTracker uses a **content-addressable storage** system. Each tracked file is hashed with SHA-256 and stored once under its hash as the filename. Identical files across different versions or datasets share the same stored object automatically.

```
.data_tracker/
├── tracker.db              # SQLite database (all metadata)
├── presets_config.json     # Transform preset definitions
└── objects/                # Content-addressable file storage
    ├── a1b2c3d4...         # File content stored by SHA-256 hash
    ├── e5f6a7b8...
    └── ...
```

**Benefits:**
- Identical files are stored only once regardless of how many versions reference them
- File integrity can be verified at any time by re-hashing
- Storage grows only when genuinely new content is added

### Database Schema

**datasets**
- `id`, `name`, `message`, `created_at`

**objects**
- `hash` (SHA-256), `size`, `created_at`

**versions**
- `id`, `dataset_id`, `object_hash`, `version`, `original_path`, `message`, `created_at`

**files**
- `id`, `version_id`, `object_hash`, `relative_path`

Each version stores its primary hash (file hash for single files, directory hash for multi-file datasets) in `versions.object_hash`, and the individual file hashes in the `files` table. This allows both deduplication at the version level and reconstruction of exact directory structures.


## Use Cases

### Data Science Workflow

```bash
# Track raw data
dt add ./raw_data/ --title "experiment-1" -m "Raw sensor readings"

# Version a cleaned copy
dt update ./cleaned_data/ --name experiment-1 -m "Removed outliers and normalised"

# Review what changed
dt compare 1.0 2.0 --name experiment-1

# Export the clean version for model training
dt export ./training_data --name experiment-1 -v 2.0
```

### Automated Pipeline with Transform

```bash
# Track training data
dt add ./train.csv --title "training-set" -m "Initial training data"

# Run augmentation in a container, auto-version the result
dt transform \
  --input-data ./train.csv \
  --output-data ./augmented/ \
  --image python:3.11 \
  --command "python /input/augment.py > /output/augmented.csv" \
  --message "Applied data augmentation"

# Review the full history
dt history --name training-set --detailed
```

### Reusable Pipeline with Presets

```bash
# Edit .data_tracker/presets_config.json to add your preset, then:
dt transform \
  --input-data ./raw_sales.csv \
  --output-data ./cleaned/ \
  --preset clean-sales-data

# Same preset, different input
dt transform \
  --input-data ./raw_sales_q4.csv \
  --output-data ./cleaned_q4/ \
  --preset clean-sales-data
```


## Development

### Project Structure

```
DataTracker/
├── src/
│   └── data_tracker/
│       ├── cli.py               # CLI entry point (Click group)
│       ├── commands.py          # Click command definitions
│       ├── core.py              # Core add/update/remove/list/history logic
│       ├── metadata.py          # Rename and annotate operations
│       ├── transform.py         # Transform execution and versioning logic
│       ├── transform_preset.py  # Preset load/save/validate
│       ├── comparison.py        # Version diff and file comparison
│       ├── db_manager.py        # All SQLite operations
│       ├── docker_manager.py    # Docker container execution
│       └── file_utils.py        # File hashing, export, open, and structure display
├── tests/                       # Pytest test suite
├── pyproject.toml               # Project and dependency configuration
└── README.md
```

### Running Tests

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest

# Run with coverage report
pytest --cov=data_tracker --cov-report=html

# Run a specific test file
pytest tests/test_core.py -v
```


### Contributing

Contributions are welcome. Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Write tests for any new functionality
4. Ensure all tests pass (`pytest`)
5. Submit a pull request


## Troubleshooting

**"Data tracker is not initialized"**
Run `dt init` in the directory where you want to track data, or in a parent directory.

**"Docker is not installed or not found in PATH"**
Install Docker Desktop (Windows/Mac) or Docker Engine (Linux) and verify with `docker --version`.

**"Dataset with name 'X' does not exist"**
Run `dt ls` to see all tracked datasets and their IDs. Use `--id` if you are unsure of the exact name.

**"Transformation completed but output directory is empty"**
Your command did not write any files to `/output`. Check that your command references `/output` correctly and that it actually produces output files.

**"Permission denied" during transform**
On Linux/Mac, check file permissions on the input and output directories. On Windows, ensure Docker Desktop has access to the relevant drives in its settings.


## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with:
- [Click](https://click.palletsprojects.com/) — CLI framework
- [SQLite](https://www.sqlite.org/) — Embedded database
- [Colorama](https://github.com/tartley/colorama) — Cross-platform terminal colours
- [Docker](https://www.docker.com/) — Containerisation
