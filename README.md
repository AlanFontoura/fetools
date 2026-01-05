# FE Tools

The main goal of this repo is to share generic tools with the Financial Engineering team.

## Quick Start

1. Download and install Docker Desktop - either Windows or Mac versions will work
2. Download and install VS Code - same as above
3. Install and enable the Dev Containers extension to VS Code
4. Clone the repo to your local machine
5. Open Docker. Just leave it running in background
6. Open it in VS Code. When asked to 'Reopen in container', click OK

## Interacting with AWS

1. Click on Google Apps in your browser and go to d1g1t-aws-identity-center
2. Log in and click on d1g1t-prod
3. Click on 'Access keys'
4. Under the tab **macOS and Linux** (even if you are on Windows), go to the first box, *Option 1: Set AWS environment variables* and click on the *Copy* button on its right
5. Paste it on your shell **after** the *venv* is automatically activated
6. Now you can upload and download data directly from AWS (for example, using `pd.read_csv("s3://bucket/file.csv")`)

## fetools

- The tools are designed in a package (auto-installed by the container) named `fetools`. All required dependencies are already installed as well.
- Inside a python terminal, you can either use `import fetools as fe` or use a single tool, like `from fetools.scripts.vnf import VnFFileGenerator`
- Alternatively, you can use the tools from the command line interface, like `python src/fetools/scripts/vnf/vnf_v52.py --params here`


# FE Tools

**Shared financial engineering tools for the d1g1t Financial Engineering team.**

## Quick Start (2 minutes)

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows/Mac)
2. Install [VS Code](https://code.visualstudio.com/)
3. Install [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
4. `git clone <repo> && cd fe-tools && code .`
5. **VS Code**: `Ctrl+Shift+P` → "Dev Containers: Reopen in Container" → **Click OK**
6. **Docker runs in background automatically**

✅ **Python 3.12 + fetools + AWS deps pre-installed**

## AWS Access (Daily 12hr Token)

1. Browser: **Google Apps** → **d1g1t-aws-identity-center** → **d1g1t-prod** → **Access keys**
2. **macOS/Linux tab** → **Option 1** → **Copy** environment variables
3. **Container terminal** (venv auto-activated): **Paste**
4. ✅ **S3 access works instantly** (`pd.read_csv("s3://bucket/file.csv")`)

**No restart needed**—creds active for 12 hours.

## Using fetools

**Python** (IPython/VS Code/Jupyter):
```python
import fetools as fe
from fetools.scripts.vnf import VnFFileGenerator
from fetools.preprocess.gresham import PreProcessVnFData
