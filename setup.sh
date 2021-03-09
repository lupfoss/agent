#!/usr/bin/env bash

CONFIG_FILE=config.py

sudo apt update
sudo apt install python3-pip

pip3 install virtualenv
cp config_template.py $CONFIG_FILE

echo; echo "****** TODO: please set up values in $CONFIG_FILE *******"; echo
