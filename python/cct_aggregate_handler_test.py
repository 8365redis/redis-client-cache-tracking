import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC
import time


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_basic_ft_aggregate_handler_basic():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    # UPDATE DATA
    d = cct_prepare.generate_single_object(10 , 10, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    r.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    # ADD A NEW DATA
    d = cct_prepare.generate_single_object(1 , 1, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(6)
    r.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert str(res) == '''OK'''

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[6, ['User.ID', '1'], ['User.ID', '10'], ['User.ID', '1001']]'''


    # DELETE DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(6)
    r.delete(key, Path.root_path())

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002']]'''

def test_basic_ft_aggregate_handler_multi_request_single_client():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert str(res) == '''OK'''

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    time.sleep(2)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    # UPDATE DATA
    d = cct_prepare.generate_single_object(10 , 10, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    r.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '10']]'''

def test_basic_ft_aggregate_handler_single_request_multi_client():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)
    client2 = connect_redis()
    resp = client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_APP_NAME_2 +  " 2")
    assert cct_prepare.OK in str(resp)
    client3 = connect_redis()
    resp = client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3 + " " + cct_prepare.TEST_APP_NAME_3 +  " 2")
    assert cct_prepare.OK in str(resp)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''

    # UPDATE DATA
    d = cct_prepare.generate_single_object(10 , 10, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    r.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''



def test_basic_ft_aggregate_handler_multi_request_multi_client():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)
    client2 = connect_redis()
    resp = client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_APP_NAME_2 +  " 2")
    assert cct_prepare.OK in str(resp)
    client3 = connect_redis()
    resp = client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3 + " " + cct_prepare.TEST_APP_NAME_3 +  " 2")
    assert cct_prepare.OK in str(resp)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    # UPDATE DATA
    d = cct_prepare.generate_single_object(10 , 10, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    r.json().set(key, Path.root_path(), d)

    time.sleep(1.1)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '10']]'''

    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001']]'''
    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '10']]'''

    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '10'], ['User.ID', '1001']]'''
    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '10']]'''