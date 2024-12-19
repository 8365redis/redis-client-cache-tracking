import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from cct_test_utils import get_redis_snapshot 
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
import time

CCT_SUBSCRIBED_INDEX = "CCT2:SIDX"

@pytest.fixture(autouse=True)
def before_and_after_test():
    yield
    r = connect_redis()
    cct_prepare.flush_db(r)
    kill_redis()

def test_basic_subscribe_index_test_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    time.sleep(1.1)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for i in range(0, 3):
        d = cct_prepare.generate_single_object(1000 - i , 2000 + i, passport_value)
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(i))

    time.sleep(0.2)

    resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    print(resp)

    time.sleep(0.1)

    resp = r.execute_command("SMEMBERS " + CCT_SUBSCRIBED_INDEX)
    print(resp)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    resp = r.execute_command("SISMEMBER " + CCT_SUBSCRIBED_INDEX + " " + cct_prepare.TEST_INDEX_NAME)
    print(resp)
    assert resp == 1

    # SUBSCRIBE
    stream_name = client1.execute_command("CCT2.SUBSCRIBE_TO_INDEX " + cct_prepare.TEST_INDEX_NAME)
    print(stream_name)
    assert cct_prepare.TEST_INDEX_NAME in str(stream_name)

    # READ all stream data
    from_stream1 = client1.xread( streams={stream_name:0} )
    print(from_stream1)

    client1.xtrim(stream_name , 0)

    time.sleep(0.1)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1005 , 2005, "fff")
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(5), Path.root_path(), d)

    # READ all stream data
    from_stream1 = client1.xread( streams={stream_name:0} )
    print(from_stream1)

    client1.xtrim(stream_name , 0)

    r.delete(cct_prepare.TEST_INDEX_PREFIX + str(1))

    time.sleep(0.1)

    # READ all stream data
    from_stream1 = client1.xread( streams={stream_name:0} )
    print(from_stream1)



def test_enable_index_subscription_called_twice_while_index_is_being_setup():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    time.sleep(1.1)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for i in range(0, 3):
        d = cct_prepare.generate_single_object(1000 - i , 2000 + i, passport_value)
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(i))

    time.sleep(0.2)

    resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    assert cct_prepare.OK in str(resp)
    # try to enable index subscription again
    try:    
        resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    except redis.exceptions.ResponseError as e:
        assert "Index is being setup" in str(e)

    time.sleep(0.1)

def test_enable_index_subscription_called_twice_while_index_already_setup():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    time.sleep(1.1)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for i in range(0, 3):
        d = cct_prepare.generate_single_object(1000 - i , 2000 + i, passport_value)
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(i))

    time.sleep(0.2)

    resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    assert cct_prepare.OK in str(resp)

    time.sleep(0.2)
    # try to enable index subscription again
    try:    
        resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    except redis.exceptions.ResponseError as e:
        assert "Index already setup" in str(e)

    time.sleep(0.1)


def test_subscribe_index_called_twice_while_index_is_being_setup():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    time.sleep(1.1)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for i in range(0, 7):
        d = cct_prepare.generate_single_object(1000 - i , 2000 + i, passport_value)
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(i))

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    time.sleep(0.2)

    resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    assert cct_prepare.OK in str(resp)

    try:    
        resp = client1.execute_command("CCT2.SUBSCRIBE_TO_INDEX "+ cct_prepare.TEST_INDEX_NAME)
    except redis.exceptions.ResponseError as e:
        assert "Index not supported" in str(e)

    time.sleep(0.1)

    resp = client1.execute_command("CCT2.SUBSCRIBE_TO_INDEX "+ cct_prepare.TEST_INDEX_NAME)
    print(resp)
    assert cct_prepare.TEST_INDEX_NAME in str(resp)

    time.sleep(0.1)


def test_subscribe_index_called_twice_while_index_is_not_supported():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    time.sleep(1.1)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for i in range(0, 7):
        d = cct_prepare.generate_single_object(1000 - i , 2000 + i, passport_value)
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(i))

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    time.sleep(0.2)

    resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    assert cct_prepare.OK in str(resp)

    try:    
        resp = client1.execute_command("CCT2.SUBSCRIBE_TO_INDEX "+ "NON_EXISTING_INDEX")
    except redis.exceptions.ResponseError as e:
        assert "Index not supported" in str(e)

    time.sleep(0.1)


def test_repopulate_command_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    time.sleep(1.1)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for i in range(0, 3):
        d = cct_prepare.generate_single_object(1000 - i , 2000 + i, passport_value)
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(i))

    time.sleep(0.2)

    resp = r.execute_command("CCT2.ENABLE_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    print(resp)

    time.sleep(0.1)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # SUBSCRIBE
    stream_name = client1.execute_command("CCT2.SUBSCRIBE_TO_INDEX " + cct_prepare.TEST_INDEX_NAME)
    print(stream_name)
    assert cct_prepare.TEST_INDEX_NAME in str(stream_name)


    resp = client1.execute_command("CCT2.REPOPULATE_INDEX_STREAM " + cct_prepare.TEST_INDEX_NAME)
    assert cct_prepare.OK in str(resp)
