#!/bin/bash
set -e
# Create virtualenv for DO Functions with Python 3.11
VENV_DIR="$(cd "$(dirname "$0")/../../../.venv" && pwd)"
"$VENV_DIR/bin/virtualenv" --without-pip virtualenv
# Use uv for pip operations
uv pip install -r requirements.txt --target virtualenv/lib/python3.11/site-packages
