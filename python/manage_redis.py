import subprocess
import time
import redis
import os
from constants import REMOTE_REDIS_CONNECTION


def kill_redis():
    if not REMOTE_REDIS_CONNECTION:
        bash_command = "redis-cli shutdown"
        subprocess.Popen(bash_command.split(),
                         stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL,
                         start_new_session=True)
        time.sleep(1)


def start_redis():
    current_working_directory = os.getcwd()
    module = current_working_directory + "/bin/cct2.so"
    bashCommand = "redis-stack-server --loadmodule " + module
    subprocess.Popen(bashCommand.split(), 
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True)
    time.sleep(2)


def start_redis_without_module():
    if not REMOTE_REDIS_CONNECTION:
        bash_command = "redis-stack-server"
        subprocess.Popen(bash_command.split(),
                         stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL,
                         start_new_session=True)
        time.sleep(2)


def connect_redis_with_start():
    start_redis()
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r


def connect_redis_with_start_without_module():
    start_redis_without_module()
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r


def connect_redis():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r
