import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_MODULE_TRACKING_PREFIX

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_updated_key_added_no_affect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "bbb"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "ccc")
    producer.json().set(key, Path.root_path(), d)

    # Check stream is empty
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # Check new key is not tracked    
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + cct_prepare.TEST_INDEX_PREFIX + str(2), cct_prepare.TEST_APP_NAME_1)
    assert not tracked_key 

def test_updated_key_matches_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "bbb"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in stream 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert key in str(from_stream[0][1])

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 

def test_updated_key_doesnt_match_any_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = passport_value
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    not_matching_data = "ccc"
    d = cct_prepare.generate_single_object(1000 , 2000, not_matching_data)
    producer.json().set(key, Path.root_path(), d)

    # Check stream is not empty
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert key in str(from_stream[0][1])

    # Check new key is not tracked anymore   
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key, cct_prepare.TEST_APP_NAME_1)
    assert not tracked_key  