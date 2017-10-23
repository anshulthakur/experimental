#!/bin/bash

/home/craft/scripts/schedule/format_file.py
sleep 1
gedit /home/craft/scripts/schedule/feedback.txt > /home/anshul/scripts/logs.txt 2>&1
gedit /home/craft/scripts/Activity.txt > /home/craft/scripts/logs.txt 2>&1
