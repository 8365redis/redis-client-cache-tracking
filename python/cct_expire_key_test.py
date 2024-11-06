import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_K2C, CCT_Q2C, CCT_EOS, CCT_DELI

KEY_EXPIRE_SECOND = 1
KEY_EXPIRE_WAIT_SECOND = 1.1

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

def test_key_expired_no_affect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    TEST_APP_NAME_1 = "test_key_expired_no_affect"

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(1), KEY_EXPIRE_SECOND, nx = True)
    passport_value = "bbb"
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(2), KEY_EXPIRE_SECOND, nx = True)

    # FIRST CLIENT
    passport_value = "ccc"
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " +  TEST_APP_NAME_1 + " " + TEST_APP_NAME_1 + " " +str(4))
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # PASS TIME TO EXPIRE KEY
    time.sleep(KEY_EXPIRE_WAIT_SECOND)

    # Check stream is empty
    from_stream = client1.xread( count=2, streams={TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # Check new key is not tracked    
    tracked_key = producer.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(1), TEST_APP_NAME_1)
    assert not tracked_key 

    # Check query is tracked    
    query_part = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:" + passport_value 
    tracked_query = producer.sismember(CCT_Q2C + query_part, TEST_APP_NAME_1)
    assert tracked_query 


def test_key_expired_affects_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    TEST_APP_NAME_1 = "test_key_expired_affects_query"

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(key, Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(1), KEY_EXPIRE_SECOND, nx = True)
    passport_value = "bbb"
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(2), KEY_EXPIRE_SECOND, nx = True)

    # FIRST CLIENT
    passport_value = "aaa"
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1 + " " + TEST_APP_NAME_1 + " " + str(4))
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # PASS TIME TO EXPIRE KEY
    time.sleep(KEY_EXPIRE_WAIT_SECOND)

    # Check deleted key is not tracked anymore   
    tracked_key = producer.sismember(CCT_K2C + key, TEST_APP_NAME_1)
    assert not tracked_key 

    # Check key is in streams 
    from_stream = client1.xread( count=2, streams={TEST_APP_NAME_1:0} )
    #print(from_stream)
    assert key in str(from_stream[0][1])

    # Check query is tracked    
    query_part = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:" + passport_value 
    tracked_query = producer.sismember(CCT_Q2C + query_part, TEST_APP_NAME_1)
    assert tracked_query 
