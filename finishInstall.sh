#!/bin/bash

python3 -m venv mindsdb
source mindsdb/bin/activate

pip install -r requirements.txt

python3 setup.py develop
