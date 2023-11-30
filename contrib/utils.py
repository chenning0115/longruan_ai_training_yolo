import os, sys
sys.path.append('../')
from contrib import taskdb
import psutil
from datetime import datetime


def time_now():
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_datetime

def check_file_exists(path):
    return os.path.exists(path)
