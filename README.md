# fetools
FE tools

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