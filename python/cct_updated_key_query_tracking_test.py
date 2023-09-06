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


def test_updated_key_doesnt_match_old_query_but_match_new_query():
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
    query_value = "aaa"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # SECOND CLIENT
    client2 = connect_redis()
    client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    new_value = "bbb"
    client2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + new_value + "}")   

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, new_value)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in streams 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(from_stream)
    assert key in str(from_stream[0][1])
    from_stream = client2.xread( count=2, streams={cct_prepare.TEST_APP_NAME_2:0} )
    #print(from_stream)
    assert key in str(from_stream[0][1])

    # Check new key is tracked
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key, cct_prepare.TEST_APP_NAME_2)
    assert tracked_key 
    
    # Add More data to stream
    d = cct_prepare.generate_single_object(1001 , 2001, new_value)
    producer.json().set(key, Path.root_path(), d)

def test_updated_key_match_new_query_while_not_mathing_old_matching_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value_1 = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value_1)
    producer.json().set(key_1, Path.root_path(), d)
    
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2)
    passport_value_2 = "bbb"
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value_2)
    producer.json().set(key_2, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    query_value = "1000"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + query_value + "}") # match first

    query_value = "bbb"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") # match second

    # UPDATE DATA
    new_value = "bbb"
    d = cct_prepare.generate_single_object(1002 , 2002, new_value)
    producer.json().set(key_1, Path.root_path(), d)

    # Check key is in streams 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert key_1 in str(from_stream[0][1])

    # Check both keys are tracked
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key_1, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key_2, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
   

def test_updated_key_match_multiple_queries_one_client():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value_1 = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value_1)
    producer.json().set(key_1, Path.root_path(), d)
    
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2)
    passport_value_2 = "bbb"
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value_2)
    producer.json().set(key_2, Path.root_path(), d)

    key_3 = cct_prepare.TEST_INDEX_PREFIX + str(3)
    passport_value_3 = "ccc"
    d = cct_prepare.generate_single_object(1002 , 2002, passport_value_3)
    producer.json().set(key_3, Path.root_path(), d)
    
    key_4 = cct_prepare.TEST_INDEX_PREFIX + str(4)
    passport_value_4 = "ddd"
    d = cct_prepare.generate_single_object(1003 , 2003, passport_value_4)
    producer.json().set(key_4, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    query_value = "1000"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + query_value + "}") # match first

    query_value = "bbb"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") # match second    

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2002, "bbb")
    producer.json().set(key_3, Path.root_path(), d)

    # Check first three keys are tracked
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key_1, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key_2, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + key_3, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key

    # Check key is in streams 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert key_3 in str(from_stream[0][1])

    # Check query is in streams 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "User\\.ID:1000 User\\.PASSPORT:bbb" in str(from_stream[0][1][0][1]["queries"])