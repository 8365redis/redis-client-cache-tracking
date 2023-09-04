import pytest
import redis
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_MODULE_TRACKING_PREFIX, CCT_MODULE_QUERY_PREFIX

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_multi_register_client():
    producer = connect_redis()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # REGISTER CLIENT1
    client1 = connect_redis()
    resp = client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    print(resp)

    # REGISTER CLIENT2
    client2 = connect_redis()
    resp = client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)
    print(resp)

def test_multi_register_ignored():
    producer = connect_redis()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # REGISTER CLIENT1
    client1 = connect_redis()
    resp = client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    print(resp)

    # REGISTER 2. Attempt CLIENT1
    try:
        resp = client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    except redis.exceptions.ResponseError as e:
        assert cct_prepare.DUPLICATE in str(e)        

    # REGISTER CLIENT2
    client2 = connect_redis()
    resp = client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)
    print(resp)    

    # REGISTER 2. Attempt CLIENT2
    try:
        resp = client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    except redis.exceptions.ResponseError as e:
        assert cct_prepare.DUPLICATE in str(e)


def test_multi_register_client_and_disconnect():
    producer = connect_redis()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # REGISTER CLIENT1
    client1 = connect_redis()
    resp = client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    print(resp)

    # REGISTER CLIENT2
    client2 = connect_redis()
    resp = client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)
    print(resp)

    client1.close()
    client2.close()

    # REGISTER CLIENT3
    client3 = connect_redis()
    resp = client3.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_3)
    assert cct_prepare.OK in str(resp)
    print(resp)