#!/bin/bash

/home/craft/scripts/schedule/format_file.py
sleep 1
gedit /home/craft/scripts/schedule/schedule.md >> /home/anshul/scripts/logs.txt 2>&1
