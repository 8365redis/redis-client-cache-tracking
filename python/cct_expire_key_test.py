import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_MODULE_TRACKING_PREFIX, CCT_MODULE_QUERY_PREFIX, CCT_MODULE_CLIENT_QUERY_PREFIX

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_key_expired_no_affect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(1), 3, nx = True)
    passport_value = "bbb"
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(2), 3, nx = True)

    # FIRST CLIENT
    passport_value = "ccc"
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # PASS TIME TO EXPIRE KEY
    time.sleep(4)

    # Check stream is empty
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # Check new key is not tracked    
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + cct_prepare.TEST_INDEX_PREFIX + str(1), cct_prepare.TEST_APP_NAME_1)
    assert not tracked_key 

    # Check query is tracked    
    query_part = "User\\.PASSPORT:" + passport_value 
    tracked_query = producer.sismember(CCT_MODULE_QUERY_PREFIX + query_part, cct_prepare.TEST_APP_NAME_1)
    assert tracked_query 


def test_key_expired_affects_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(key, Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(1), 3, nx = True)
    passport_value = "bbb"
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)
    producer.expire(cct_prepare.TEST_INDEX_PREFIX + str(2), 3, nx = True)

    # FIRST CLIENT
    passport_value = "aaa"
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # PASS TIME TO EXPIRE KEY
    time.sleep(4)

    # Check deleted key is not tracked anymore   
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key, cct_prepare.TEST_APP_NAME_1)
    assert not tracked_key 

    # Check key is in streams 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert key in str(from_stream[0][1])

    # Check query is tracked    
    query_part = "User\\.PASSPORT:" + passport_value 
    tracked_query = producer.sismember(CCT_MODULE_QUERY_PREFIX + query_part, cct_prepare.TEST_APP_NAME_1)
    assert tracked_query 
