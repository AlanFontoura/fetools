# FE Tools - AI Coding Agent Instructions

## Project Overview

Financial engineering toolkit for transforming client data into standardized importers for portfolio management systems. Core workflows: **Partial Ownership/SMA setup**, **Values & Flows (VnF) file generation**, and **Compliance Reporting**.

## Architecture & Data Flow

### 1. Input → Transform → Output Pattern
- **Inputs**: CSV/Parquet files in `data/inputs/{tool_name}/{client_name}/`
- **Configs**: TOML files (preferred) or JSON in `data/configs/` or `data/inputs/{tool}/{client}/`
- **Transform**: Configuration-driven column mappings via `DataTransformer` classes
- **Outputs**: CSV/Excel files in `data/outputs/{tool_name}/{client_name}/`

### 2. Modular Package Structure
```
src/fetools/
├── tools/           # Core workflow modules (vnf, po_sma, compliance)
├── api/             # API clients (d1g1t integration)
├── transformers/    # Data transformation utilities
├── cli/             # Command-line interface (typer)
└── exceptions.py    # Custom exceptions
```

### 3. Configuration-Driven Transformations
All data transformations follow this pattern:
```python
# Column mapping: source column → target column with prefix/suffix
config = {
    "target_col": {"source": "source_col", "prefix": "foo_", "suffix": "_bar"},
    "constant_col": {"value": "FIXED_VALUE"}
}
```

## CLI-First Architecture

### Primary Interface
Users interact via `fetools` CLI commands:
```bash
fetools vnf run {client}              # Run VnF workflow
fetools po-sma generate               # Generate PO/SMA files
fetools compliance report             # Generate compliance report
```

### Python API (Secondary)
All tools also available as importable classes:
```python
from fetools import ValuesAndFlowsTools, ComplianceReport, PROJECT_ROOT
```

## Key Components

### Ownership Chain Calculator
- **Function**: `extend_ownership_table()` in [po_sma_loader.py](../src/fetools/tools/po_sma_loader.py)
- **Purpose**: Recursively calculates multi-level ownership percentages (e.g., Fund → Class → Client → Household)
- **Algorithm**: Iteratively merges DataFrame on `Owned=Owner` until no new entries found

### VnF Data Processing
- **Main class**: `ValuesAndFlowsTools` in [vnf.py](../src/fetools/tools/vnf.py)
- **TOML config**: Located in `data/inputs/vnf/<client>/vnf.toml`
- **Key operations**:
  - Filters data up to `stitch_date`
  - Adjusts last date to `stitch_date - 1 day`
  - Creates input files (positions), portfolio configs, book values
- **Data requirements**: `Date`, `Account ID`, `Market Value`, `Net Transfers`, `Net Internal Transfers`, `Household ID`

### File Generator Pattern
All file generators use `DataTransformer` or similar patterns:
1. Load config (JSON/TOML)
2. Apply `transform()` with column configs
3. Select final columns in config order
4. Save to `data/outputs/` with folder structure from config

## Development Workflow

### Environment Setup
1. **Container auto-activates venv** at `/workspaces/fetools/.venv/`
2. **Install package**: `pip install -e .[dev]` (editable mode, changes live instantly)
3. **Dependencies**: `pandas`, `awswrangler`, `dataclass-binder`, `typer` (see pyproject.toml)

### AWS S3 Access
- **12-hour tokens** from AWS Identity Center (copy/paste env vars in terminal)
- Direct S3 paths work: `pd.read_csv("s3://bucket/file.csv")`
- No credential files or restarts needed

### Code Style
- **Black formatting**: 78 char line length (configured in pyproject.toml)
- **Python 3.12+** required
- **Type hints**: Use but mypy has `disallow_untyped_defs = false`

### Running Scripts
```bash
# CLI commands (preferred)
fetools vnf run gresham
fetools po-sma generate
fetools compliance report

# As module (from project root)
python -m fetools.tools.vnf

# As package (IPython/notebooks)
from fetools import ValuesAndFlowsTools, PROJECT_ROOT
```

## Common Patterns

### DataFrame Column Transformations
```python
# Pattern 1: Copy with prefix/suffix
df["new_col"] = df["old_col"].apply(lambda x: f"prefix_{x}_suffix")

# Pattern 2: Constant value column
df["new_col"] = "CONSTANT_VALUE"

# Pattern 3: Config-driven (prefer this)
DataTransformer(df, loader_config).transform()
```

### Config File Structures
- **JSON configs**: `data/configs/` for PO/SMA (see sma_mapping.json)
- **TOML configs**: `data/inputs/vnf/` for VnF workflows (client-specific)
- **Templates**: `data/templates/` for example configs
- Both follow same column mapping pattern: `source`/`value`, `prefix`/`suffix`

### Path Conventions
- Use `Path` objects from `pathlib` (not string concatenation)
- Use `PROJECT_ROOT` constant from `fetools` package for relative paths
- Input paths: `PROJECT_ROOT / "data/inputs/<workflow>/<file>"`
- Output paths: `PROJECT_ROOT / "data/outputs/<entity_type>/<file>"`
- Auto-create dirs: `path.mkdir(parents=True, exist_ok=True)`

## Critical Notes

- **CLI-first design**: All tools accessible via `fetools` command
- **Config files drive logic**: Always check existing JSON/TOML before hardcoding transformations
- **Date handling**: VnF uses `stitch_date - 1 day` for last date to avoid off-by-one errors
- **DataFrame operations**: Most transforms are config-driven, avoid manual column manipulation
- **AWS dependencies**: `awswrangler` is installed but primarily for S3 access via pandas
- **PROJECT_ROOT**: Use this constant for all relative paths instead of hardcoded strings

## When Extending

1. **New data source**: Add config file to `data/configs/` or `data/inputs/`
2. **New transformer**: Add to `src/fetools/transformers/`
3. **New tool**: Create in `src/fetools/tools/` and add CLI command in `src/fetools/cli/main.py`
4. **New public function**: Add to `src/fetools/__init__.py` `__all__` list
5. **New client workflow**: Create client folder in `data/inputs/{tool}/<client>/` with TOML config
