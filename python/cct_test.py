import subprocess
import redis
import time
import pytest
from redis.commands.json.path import Path
import cct_prepare

def kill_redis():
    bashCommand = "redis-cli shutdown"
    process = subprocess.Popen(bashCommand.split(), 
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                start_new_session=True)    
    time.sleep(1)

def start_redis():
    bashCommand = "redis-stack-server --loadmodule ./bin/cct.so"
    subprocess.Popen(bashCommand.split(), 
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True)
    time.sleep(2)


@pytest.fixture(autouse=True)
def before_and_after_test():
    connect_redis()
    yield
    kill_redis()

def connect_redis():
    start_redis()
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    return r

def test_unique_id_tracking_test_1():
    r = connect_redis()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)
    data = cct_prepare.generate_input(10)
    cct_prepare.add_list(r, data)

    
    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    print(resp)
    
    # SEARCH
    print(data[0])
    resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "}")
    print(resp)

    # CHANGE first item
    id = resp[0]
    new_d = {}
    cct_prepare.generate_object(new_d,12,22)
    resp = r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(id), Path.root_path(), new_d)
    print(resp)

    # CHECK STREAM
    resp = r.xread( count=1, streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(resp)   


def test_multi_register_handle_1():
    r = connect_redis()
    cct_prepare.flush_db(r) # clean all db first

    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # REGISTER 2. Attempt 
    try:
        resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    except redis.exceptions.ResponseError as e:
        assert cct_prepare.DUPLICATE in str(e)

    # REGISTER 3. Attempt
    try:
        resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    except redis.exceptions.ResponseError as e:
        assert cct_prepare.DUPLICATE in str(e) 

    # REGISTER with different app name from same client
    try:
        resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    except redis.exceptions.ResponseError as e:
        assert cct_prepare.DUPLICATE in str(e) 



