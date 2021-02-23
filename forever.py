# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 13:57:38 2021

@author: transitcenter
"""

from subprocess import run
from time import sleep
import traceback

# Path and name to the script you are trying to start
file_path = "pull_reliability.py" 

restart_timer = 2
def start_script():
    try:
        # Make sure 'python' command is available
        run("python3 " + file_path, check=True, shell=True) 
    except Exception:
        # Script crashed, lets restart it!
        traceback.print_exc()
        handle_crash()

def handle_crash():
    print("Script Crashed")  # Going to need to log this eventually!
    sleep(restart_timer)  # Restarts the script after 2 seconds
    start_script()

start_script()