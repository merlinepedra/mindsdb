#!/bin/bash

python3 -m venv mindsdb
source mindsdb/bin/activate

pip3 install wheel
pip3 install -r requirements.txt

python3 setup.py --verbose develop
