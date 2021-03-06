#!/usr/bin/env bash

while true; do
  git pull origin master
  python3 -m virtualenv ~/.lupvenv
  source ~/lupvenv/bin/activate
  pip3 install -r requirements.txt
  python3 agent.py 5
done
