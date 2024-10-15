#!/bin/sh

input="balanced://arclink/acqui1:18001;arclink/acqui2:18001"

scpython polling-pick-client.py -H proc -I "$input" --debug
