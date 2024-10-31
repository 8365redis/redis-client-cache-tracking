import pytest
import time
import redis
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, SKIP_HB_TEST,  \
                CCT_NOT_REGISTERED_COMMAND_ERROR, CCT_HEART_BEAT_INTERVAL

import constants

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_heartbeat_normal():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    client1 = connect_redis()
    res = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)

@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_heartbeat_client_expire():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    client1 = connect_redis()
    res = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    time.sleep(CCT_HEART_BEAT_INTERVAL * 3 + 1)
    try:
        res = client1.execute_command("CCT2.HEARTBEAT")
    except redis.exceptions.ResponseError as e:
        assert CCT_NOT_REGISTERED_COMMAND_ERROR in str(e)

@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_expired_client_stream_empty():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    time.sleep(CCT_HEART_BEAT_INTERVAL * 4)
    try:
        client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    except redis.exceptions.ResponseError as e:
        assert CCT_NOT_REGISTERED_COMMAND_ERROR in str(e)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2001 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream is empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream


@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_expired_client_stream():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2001 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])
    
    time.sleep(CCT_HEART_BEAT_INTERVAL * 4)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2001 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    try:
        client1.execute_command("CCT2.HEARTBEAT")
    except redis.exceptions.ResponseError as e:
        assert CCT_NOT_REGISTERED_COMMAND_ERROR in str(e)



