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

def test_basic_ft_aggregate_return_first():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

def test_basic_ft_aggregate_return_cached_same_client():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    time.sleep(2)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''


def test_basic_ft_aggregate_return_cached_different_clients():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    client2 = connect_redis()
    resp = client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)

    client3 = connect_redis()
    resp = client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3)
    assert cct_prepare.OK in str(resp)

    client4 = connect_redis()
    resp = client4.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_4)
    assert cct_prepare.OK in str(resp)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client4.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    time.sleep(4)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client4.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''