import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC, \
                CCT_QUERY_HALF_TTL, CCT_QUERY_TTL, CCT_MODULE_PREFIX
from cct_test_utils import check_query_meta_data , get_redis_snapshot

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

def test_basic_unsubscribe_1():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    TEST_INDEX_NAME = "test_basic_unsubscribe_1_index"
    TEST_INDEX_PREFIX = "test_basic_unsubscribe_1_prefix:"
    cct_prepare.create_index_with_prefix(producer, TEST_INDEX_PREFIX, TEST_INDEX_NAME)

    # ADD INITIAL DATA
    query_value = "test_basic_unsubscribe_1_query_value"
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    key_1 = TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)

    TEST_APP_NAME_1 = "test_basic_unsubscribe_1_app_1"

    # REGISTER CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    all_query = " @User\\.PASSPORT:{" + query_value + "}"
    res = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME + all_query)
    assert str(res) == '''[1, 'test_basic_unsubscribe_1_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_basic_unsubscribe_1_query_value","Address":{"ID":"2000"}}}']]'''

    # Key existence
    assert producer.exists('CCT2:QC:test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value:test_basic_unsubscribe_1_app_1') == 1
    # Set membership assertions
    assert producer.sismember('CCT2:Q2K:test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value', 'test_basic_unsubscribe_1_prefix:1') == 1
    assert producer.sismember('CCT2:Q2C:test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value', 'test_basic_unsubscribe_1_app_1') == 1
    assert producer.sismember('CCT2:C2Q:test_basic_unsubscribe_1_app_1', 'test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value') == 1
    assert producer.sismember('CCT2:K2C:test_basic_unsubscribe_1_prefix:1', 'test_basic_unsubscribe_1_app_1') == 1
    assert producer.sismember('CCT2:K2Q:test_basic_unsubscribe_1_prefix:1', 'test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value') == 1

    #UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2001, query_value)
    producer.json().set(key_1, Path.root_path(), d)

    # READ STREAM
    from_stream = client1.xread( streams={TEST_APP_NAME_1:0} )
    assert '''test_basic_unsubscribe_1_prefix:1''' in str(from_stream)

    get_redis_snapshot()

    # UNSUBSCRIBE
    #time.sleep(1.1)
    res = client1.execute_command("CCT2.FT.SEARCH.UNSUBSCRIBE " + TEST_INDEX_NAME + " " + all_query)
    assert str(res) == "OK"

    get_redis_snapshot()

    # Key existence
    assert producer.exists('CCT2:QC:test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value:test_basic_unsubscribe_1_app_1') == 0
    # Set membership assertions
    assert producer.sismember('CCT2:Q2K:test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value', 'test_basic_unsubscribe_1_prefix:1') == 0
    assert producer.sismember('CCT2:Q2C:test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value', 'test_basic_unsubscribe_1_app_1') == 0
    assert producer.sismember('CCT2:C2Q:test_basic_unsubscribe_1_app_1', 'test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value') == 0
    #assert producer.sismember('CCT2:K2C:test_basic_unsubscribe_1_prefix:1', 'test_basic_unsubscribe_1_app_1') == 1
    assert producer.sismember('CCT2:K2Q:test_basic_unsubscribe_1_prefix:1', 'test_basic_unsubscribe_1_index:User\\.PASSPORT:test_basic_unsubscribe_1_query_value') == 0

    # TRIM STREAMS
    client1.xtrim(TEST_APP_NAME_1 , 0)

    time.sleep(0.1)

    #UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2001, query_value)
    producer.json().set(key_1, Path.root_path(), d)

    # READ STREAM
    from_stream = client1.xread( streams={TEST_APP_NAME_1:0} )
    print(from_stream)

