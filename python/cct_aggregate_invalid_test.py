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


def test_aggregate_invalid_multi_request_single_client():
    r = connect_redis()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(5):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 1000")
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

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert str(res) == '''OK'''

    time.sleep(3.1)

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert str(res) == '''OK'''

    res = client1.execute_command("CCT2.INVALIDATE")
    assert str(res) == '''OK'''

    time.sleep(1.2)

    # REQUEST
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 4")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002'], ['User.ID', '1003']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 2")
    assert str(response) == '''[5, ['User.ID', '1000'], ['User.ID', '1001']]'''
    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 1")
    assert str(response) == '''[5, ['User.ID', '1000']]'''

    time.sleep(1.2)

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert str(res) == '''OK'''

    res = client1.execute_command("CCT2.INVALIDATE")
    assert str(res) == '''OK'''

    time.sleep(2.2)
