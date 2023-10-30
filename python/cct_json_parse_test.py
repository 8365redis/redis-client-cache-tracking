import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_K2C, CCT_EOS
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

from cct_test_utils import get_redis_snapshot

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_json_with_special_chars_value():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "~@#$%^&*()_+"
    id = "~@#$%^&*()_+"
    addr_id = "~@#$%^&*()_+"
    d = cct_prepare.generate_single_object( id , addr_id, passport_value)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "bbb"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(id , addr_id, query_value)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in stream 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert key in str(from_stream[0][1])

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 

def test_json_with_special_chars_search_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "1234567890\';l//"
    id = "~@#$%^&*()_+"
    addr_id = "~@#$%^&*()_+"
    d = cct_prepare.generate_single_object( id , addr_id, passport_value)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "\\^"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(id , addr_id, query_value)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in stream 
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert key in str(from_stream[0][1])

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 

def test_json_with_special_chars_multi_search_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "1234567890\';l//"
    id = "~@#$%^&*()_+"
    addr_id = "~@#$%^&*()_+"
    d = cct_prepare.generate_single_object( id , addr_id, passport_value)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    query_value = "\\^\\@\\#\\$\\%\\&\\*\\(\\(\\)\\_\\+"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(id , addr_id, query_value)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in stream
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert key in str(from_stream[0][1])

    # Check new key is tracked
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key

def test_json_with_special_chars_in_snapshot():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "SOMETHING" + "^MORE"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "@User\\.PASSPORT:{aaa}"

    # FIRST CLIENT
    query_value = "SOMETHING"
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(0.2)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_json_with_special_chars_in_snapshot_with_hit():
    TEST_INDEX_NAME = "usersJsonIdx"
    TEST_INDEX_PREFIX = "users:"
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    schema = (TagField("$.Key1.RIC", as_name="Key1.RIC"))
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    d = {}
    d["Key1"] = {}
    key_val = "FEIV3^2"
    d["Key1"]["RIC"] = key_val
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)

    # FIRST CLIENT
    query_value = "FEIV3"
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @Key1\\.RIC:{" + query_value + "}")

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(0.2)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    #QUERY AGAIN
    key_val = "FEIV3\\^2"
    res = client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @Key1\\.RIC:{" + key_val + "}")
    print(res)

    get_redis_snapshot()

