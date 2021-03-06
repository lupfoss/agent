#!/usr/bin/env bash

CONFIG_FILE=config.py

pip3 install virtualenv
cp config_template.py $CONFIG_FILE

echo; echo "****** TODO: please set up values in $CONFIG_FILE *******"; echo
