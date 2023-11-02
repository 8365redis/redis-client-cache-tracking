import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_K2C, CCT_Q_DELI, CCT_EOS
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType


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
    assert CCT_EOS in from_stream[0][1][0][1]

    # Check new key is not tracked    
    tracked_key = producer.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(2), cct_prepare.TEST_APP_NAME_1)
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
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_APP_NAME_1)
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
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_APP_NAME_1)
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
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_APP_NAME_2)
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
    tracked_key = producer.sismember(CCT_K2C + key_1, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = producer.sismember(CCT_K2C + key_2, cct_prepare.TEST_APP_NAME_1)
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
    tracked_key = producer.sismember(CCT_K2C + key_1, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = producer.sismember(CCT_K2C + key_2, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = producer.sismember(CCT_K2C + key_3, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key

    # Check key is in streams 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert key_3 in str(from_stream[0][1])

    # Check query is in streams 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "User\\.ID:1000" + CCT_Q_DELI +"User\\.PASSPORT:bbb" in str(from_stream[0][1][1][1]["queries"])

def test_updated_key_match_same_queries_one_client_multi_result_with_base():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    # TEST SPECIFIC SCHEME
    TEST_INDEX_NAME = "usersJsonIdx"
    TEST_INDEX_PREFIX = "users:"
    schema = (TagField("$.user.a", as_name="user.a"), TagField("$.user.b", as_name="user.b"),  \
              TagField("$.user.c", as_name="user.c"), TagField("$.user.d", as_name="user.d"))
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    #ADD DATA
    d = { "user" : {"a" : "a_data" , "b" : "b_data" , "c" : "c_data", "d" : "d_data" } }
    for i in range(1,20) :
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # SEARCH
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "a_data"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @user\\.a:{" + query_value + "}" + " LIMIT 0 100") # match first

    #UPDATE DATA
    d = { "user" : {"a" : "a_data" , "b" : "b_data2" , "c" : "c_data2", "d" : "d_data2" } }
    key = cct_prepare.TEST_INDEX_PREFIX + str(10)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in streams 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )

def test_updated_key_match_same_queries_one_client_multi_result():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    # TEST SPECIFIC SCHEME
    TEST_INDEX_NAME = "usersJsonIdx"
    TEST_INDEX_PREFIX = "users:"
    schema = (TagField("$.a", as_name="a"), TagField("$.b", as_name="b"),  \
              TagField("$.c", as_name="c"), TagField("$.d", as_name="d"))
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    #ADD DATA
    d = { "a" : "a_data" , "b" : "b_data" , "c" : "c_data", "d" : "d_data"}
    for i in range(1,20) :
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # SEARCH
    client1 = connect_redis_with_start()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "a_data"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @a:{" + query_value + "}" + " LIMIT 0 100") # match first

    #UPDATE DATA
    d = { "a" : "a_data" , "b" : "b_data2" , "c" : "c_data2", "d" : "d_data2"}
    key = cct_prepare.TEST_INDEX_PREFIX + str(10)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in streams 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )


def test_updated_key_match_same_queries_one_client_no_root():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    # TEST SPECIFIC SCHEME
    TEST_INDEX_NAME = "usersJsonIdx"
    TEST_INDEX_PREFIX = "users:"
    schema = (TagField("$.a", as_name="a"), TagField("$.b", as_name="b"),  \
              TagField("$.c", as_name="c"), TagField("$.d", as_name="d"))
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    #ADD DATA
    d = { "a" : "a_data" , "b" : "b_data" , "c" : "c_data", "d" : "d_data"}
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # SEARCH
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "a_data"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @a:{" + query_value + "}" + " LIMIT 0 100") # match first

    #UPDATE DATA
    d = { "a" : "a_data" , "b" : "b_data2" , "c" : "c_data2", "d" : "d_data2"}
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in streams 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )


def test_updated_key_match_same_queries_one_client_mixed_data():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    # TEST SPECIFIC SCHEME
    TEST_INDEX_NAME = "usersJsonIdx"
    TEST_INDEX_PREFIX = "users:"
    schema = (TagField("$.a", as_name="a"), TagField("$.b", as_name="b"),  \
              TagField("$.c", as_name="c"), TagField("$.d.d_1", as_name="d.d_1"))
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    #ADD DATA
    d = { "a" : "a_data" , "b" : "b_data" , "c" : "c_data", "d" : { "d_1" : "d_1_data" , "d_2" : "d_2_data" }}
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # SEARCH
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "d_1_data"
    res = client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @d\\.d_1:{" + query_value + "}" + " LIMIT 0 100") # match first
    print(res)

    #UPDATE DATA
    d = { "a" : "a_data2" , "b" : "b_data2" , "c" : "c_data2", "d" : { "d_1" : "d_1_data" , "d_2" : "d_2_data2" }}
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in streams 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)


def test_keys_delete_while_client_offline_snapshot_operation():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1)
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    producer.json().set(key_1, Path.root_path(), d)
    
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2)
    d = cct_prepare.generate_single_object(1000 , 2001, "bbb")
    producer.json().set(key_2, Path.root_path(), d)

    key_3 = cct_prepare.TEST_INDEX_PREFIX + str(3)
    d = cct_prepare.generate_single_object(1000 , 2002, "ccc")
    producer.json().set(key_3, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    query_value = "1000"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + query_value + "}") # match all

    # DISCONNECT
    client1.connection_pool.disconnect()

    #DELETE 2 KEYS
    producer.delete(key_2)
    producer.delete(key_3)

    # FIRST CLIENT RECONNECT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)