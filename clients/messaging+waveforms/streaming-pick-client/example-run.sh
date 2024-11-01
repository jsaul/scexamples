#!/bin/sh

# streaming waveform data input
sinput="balanced://slink/acqui1:18000;slink/acqui2:18000"

# archive waveform data input
ainput="balanced://arclink/acqui1:18001;arclink/acqui2:18001"

scpython streaming-pick-client.py -H proc -I "$sinput" --archive-input "$ainput" --export-dir /dev/shm/data --debug
