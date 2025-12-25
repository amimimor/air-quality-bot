#!/bin/bash
set -e
# Create virtualenv for DO Functions with Python 3.11
pip install --quiet virtualenv
virtualenv --without-pip virtualenv
pip install -r requirements.txt --target virtualenv/lib/python3.11/site-packages
