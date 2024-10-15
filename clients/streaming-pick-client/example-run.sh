#!/bin/sh

input="balanced://slink/acqui1:18000;slink/acqui2:18000"

scpython streaming-pick-client.py -H proc -I "$input" --export-dir /dev/shm/data --debug
