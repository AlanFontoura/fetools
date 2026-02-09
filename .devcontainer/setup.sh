#!/bin/bash
set -e

# Create the .aws folder if it doesn't exist so AWS CLI doesn't complain
mkdir -p .aws

# Setup Python Virtual Env
if [ ! -d ".venv" ]; then
    python -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]

# Node tools
npm install -g @google/gemini-cli