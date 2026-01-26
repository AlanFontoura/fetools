# FE Tools

**Shared financial engineering tools for the d1g1t FE team.**

A comprehensive toolkit for transforming client data into standardized portfolio management formats. Includes Values and Flows (VnF), Partial Ownership/SMA setup, and Compliance Reporting workflows.

## Quick Start (2 minutes)

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows/Mac)
2. Install [VS Code](https://code.visualstudio.com/)
3. Install [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
4. Clone repo
5. `cd fetools && code .`
6. **VS Code**: `Ctrl+Shift+P` → "Dev Containers: Reopen in Container" → **Click OK**
7. **Docker runs in background automatically**

✅ **Python 3.12 + fetools CLI + AWS deps pre-installed**

## Installation

The package is automatically installed in the dev container. To reinstall or update:

```bash
pip install -e .
```

This provides the `fetools` CLI command globally.

## AWS Access (Daily 12hr Token)

1. Browser: **Google Apps** → **d1g1t-aws-identity-center** → **d1g1t-prod** → **Access keys**
2. **macOS/Linux tab** → **Option 1** → **Copy** environment variables
3. **Container terminal** (venv auto-activated): **Paste**
4. ✅ **S3 access works instantly** (e.g., `pd.read_csv("s3://bucket/file.csv")`)

**No restart needed**—creds active for 12 hours.

## Using fetools

### CLI Commands

```bash
# Show all available commands
fetools --help

# Values and Flows workflow
fetools vnf run gresham                    # Run VnF for client 'gresham'
fetools vnf run client_name --config path/to/config.toml

# Partial Ownership/SMA
fetools po-sma generate                    # Generate PO/SMA files
fetools po-sma generate --output-dir custom/path/

# Compliance Reporting
fetools compliance report                  # Generate compliance report
fetools compliance report --config custom_compliance.toml
```

### Python API

**As a package:**
```python
from fetools import ValuesAndFlowsTools, ComplianceReport, PROJECT_ROOT

# Values and Flows
vnf = ValuesAndFlowsTools("data/inputs/vnf/gresham/vnf.toml")
vnf.run()

# Compliance Report
report = ComplianceReport("data/inputs/compliance/compliance.toml")
report.login()
report.run()

# Use PROJECT_ROOT for relative paths
config_path = PROJECT_ROOT / "data/configs/my_config.toml"
```

**As single tools:**
```python
from fetools.tools.vnf import ValuesAndFlowsTools
from fetools.tools.po_sma_loader import extend_ownership_table
from fetools.api.base_main import BaseMain
```

## Scripts & Tools

### 1. Values and Flows (VnF)
**Purpose:** Prepare historical position/transaction data for portfolio stitching

**CLI:** `fetools vnf run {client_name}`

**Input:** CSV with Date, Account ID, Market Value, Net Transfers, Household ID

**Output:** Household mappings, portfolio configs, book values, input files

**Config:** TOML file at `data/inputs/vnf/{client}/vnf.toml`

### 2. Partial Ownership/SMA
**Purpose:** Generate portfolio entities from ownership chains

**CLI:** `fetools po-sma generate`

**Input:** Account.csv, Client.csv, LEOwnership.csv

**Output:** Fund entities, client/account mappings, household structures

**Key Feature:** Recursively calculates multi-level ownership percentages

### 3. Compliance Reporting
**Purpose:** Generate compliance reports from d1g1t API

**CLI:** `fetools compliance report`

**Requires:** Active d1g1t API credentials + AWS tokens

**Output:** Excel file with conditional formatting for breaches

## Project Structure

```
fetools/
├── src/fetools/              # Main package
│   ├── tools/                # Core workflow modules
│   │   ├── vnf.py           # Values and Flows
│   │   ├── po_sma_tools.py  # Partial Ownership/SMA
│   │   ├── po_sma_loader.py # PO/SMA data loading
│   │   └── compliance_report.py
│   ├── api/                  # API clients (d1g1t)
│   ├── transformers/         # Data transformation utilities
│   ├── cli/                  # Command-line interface
│   └── exceptions.py
│
├── data/
│   ├── inputs/               # Client input data
│   │   ├── vnf/{client}/    # VnF client folders
│   │   ├── partial_ownership/
│   │   └── compliance/
│   ├── outputs/              # Generated outputs
│   ├── configs/              # Tool configuration files
│   └── templates/            # Example config templates
│
└── tests/                    # Unit tests (to be added)
```

## Configuration Templates

Example templates are in `data/templates/`:
- `vnf_template.toml` - Values and Flows config
- `compliance_template.toml` - Compliance report config
- `partial_ownership_template.toml` - PO/SMA config

## Troubleshooting

| Issue                     | Fix                                             |
| ------------------------- | ----------------------------------------------- |
| "No module named fetools" | Container terminal: `pip install -e .`          |
| "fetools: command not found" | Run `pip install -e .` to install CLI        |
| AWS "NoCredentialsError"  | Copy/paste daily token from AWS Identity Center |
| Slow first container      | Normal (~2min), cached after                    |
| Docker not running        | Start Docker Desktop                            |
| Import errors after restructure | Re-run `pip install -e .`                |

## Standards

✅ Black formatting (78 chars line length)

✅ Type hints with mypy

✅ CLI-first design with Python API fallback

✅ Configuration-driven transformations (TOML preferred)

✅ Changes live instantly (editable install)

✅ GitHub Codespaces compatible

## Development

See [.github/copilot-instructions.md](.github/copilot-instructions.md) for detailed architecture documentation and development guidelines.
