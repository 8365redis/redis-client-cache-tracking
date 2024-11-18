import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from cct_test_utils import get_redis_snapshot 
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC
import time

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

def test_basic_expire():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = '''test_basic_expire_app'''
    TEST_INDEX_NAME = '''test_basic_expire_index'''
    TEST_INDEX_PREFIX = '''test_basic_expire_index_prefix:'''
    cct_prepare.create_index_with_prefix(producer, TEST_INDEX_PREFIX, TEST_INDEX_NAME)

     # ADD INITIAL DATA
    passport_value = "test_basic_renew_passport"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key1 = TEST_INDEX_PREFIX + str(1)
    producer.json().set(key1, Path.root_path(), d)   


    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1) # DEBUG QUERY TTL SET TO 1 second
    query = "@User\\.PASSPORT:{" + passport_value + "}"
    res = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    assert str(res) == '''[1, 'test_basic_expire_index_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_basic_renew_passport","Address":{"ID":"2000"}}}']]'''

    meta_data_query_key = '''CCT2:QC:test_basic_renew_index:User\\.PASSPORT:test_basic_renew_passport:test_basic_renew_app'''
    assert producer.exists(meta_data_query_key) == 0

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    time.sleep(0.6)

    assert producer.exists(meta_data_query_key) == 0

def test_basic_renew():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = '''test_basic_renew_app'''
    TEST_INDEX_NAME = '''test_basic_renew_index'''
    TEST_INDEX_PREFIX = '''test_basic_renew_index:'''
    cct_prepare.create_index_with_prefix(producer, TEST_INDEX_PREFIX, TEST_INDEX_NAME)

    # ADD INITIAL DATA
    passport_value = "test_basic_renew_passport"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key1 = TEST_INDEX_PREFIX + str(1)
    producer.json().set(key1, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1) # DEBUG QUERY TTL SET TO 1 second
    res = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    assert str(res) == '''[1, 'test_basic_renew_index:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_basic_renew_passport","Address":{"ID":"2000"}}}']]'''

    meta_data_query_key = '''CCT2:QC:test_basic_renew_index:User\\.PASSPORT:test_basic_renew_passport:test_basic_renew_app'''
    assert producer.exists(meta_data_query_key) == 1

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    query_and_index_normalized = '''test_basic_renew_index:User\\.PASSPORT:test_basic_renew_passport'''
    # RENEW
    client1.execute_command("CCT2.FT.RENEW " + query_and_index_normalized)

    time.sleep(0.6)

    assert producer.exists(meta_data_query_key) == 1

    # UPDATE DATA
    d = cct_prepare.generate_single_object(9999  , 9999, passport_value)
    producer.json().set(key1, Path.root_path(), d)

    # CHECK STREAMS
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert key1 in str(from_stream[0])

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")
    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    assert producer.exists(meta_data_query_key) == 0

    # TRIM STREAMS
    client1.xtrim(TEST_APP_NAME_1 , 0)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(0000  , 0000, passport_value)
    producer.json().set(key1, Path.root_path(), d)

    # CHECK STREAMS
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert len(from_stream) == 0 # no data in stream




def test_basic_renew_with_multi_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = '''test_basic_renew_with_multi_query_app'''    
    TEST_INDEX_NAME = '''test_basic_renew_with_multi_query_index'''
    TEST_INDEX_PREFIX = '''test_basic_renew_with_multi_query_prefix:'''
    cct_prepare.create_index_with_prefix(producer, TEST_INDEX_PREFIX, TEST_INDEX_NAME)

    # ADD INITIAL DATA
    passport_value = "test_basic_renew_with_multi_query_passport"
    id_value = 1000
    address_id_value = 2000
    d = cct_prepare.generate_single_object(id_value , address_id_value, passport_value)
    key1 = TEST_INDEX_PREFIX + str(1)
    producer.json().set(key1, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1) # DEBUG QUERY TTL SET TO 1 second
    res = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    assert str(res) == '''[1, 'test_basic_renew_with_multi_query_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_basic_renew_with_multi_query_passport","Address":{"ID":"2000"}}}']]'''
    res = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.ID:{" + str(id_value) + "}")
    assert str(res) == '''[1, 'test_basic_renew_with_multi_query_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_basic_renew_with_multi_query_passport","Address":{"ID":"2000"}}}']]'''
    res = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.Address\\.ID:{" + str(address_id_value) + "}")
    assert str(res) == '''[1, 'test_basic_renew_with_multi_query_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_basic_renew_with_multi_query_passport","Address":{"ID":"2000"}}}']]'''

    meta_data_query_key_1 = '''CCT2:QC:test_basic_renew_with_multi_query_index:User\\.PASSPORT:test_basic_renew_with_multi_query_passport:test_basic_renew_with_multi_query_app'''
    assert producer.exists(meta_data_query_key_1) == 1
    meta_data_query_key_2    = '''CCT2:QC:test_basic_renew_with_multi_query_index:User\\.ID:1000:test_basic_renew_with_multi_query_app'''
    assert producer.exists(meta_data_query_key_2) == 1
    meta_data_query_key_3 = '''CCT2:QC:test_basic_renew_with_multi_query_index:User\\.Address\\.ID:2000:test_basic_renew_with_multi_query_app'''
    assert producer.exists(meta_data_query_key_3) == 1
    

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    query_and_index_normalized_1 = '''test_basic_renew_with_multi_query_index:User\\.PASSPORT:test_basic_renew_with_multi_query_passport'''
    query_and_index_normalized_2 = '''test_basic_renew_with_multi_query_index:User\\.ID:1000'''
    query_and_index_normalized_3 = '''test_basic_renew_with_multi_query_index:User\\.Address\\.ID:2000'''
    # RENEW
    client1.execute_command("CCT2.FT.RENEW " + query_and_index_normalized_1 + " " + query_and_index_normalized_2 + " " + query_and_index_normalized_3)

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")  

    assert producer.exists(meta_data_query_key_1) == 1
    assert producer.exists(meta_data_query_key_2) == 1
    assert producer.exists(meta_data_query_key_3) == 1

    # UPDATE DATA
    d = cct_prepare.generate_single_object(9999  , 9999, "9999")
    producer.json().set(key1, Path.root_path(), d)

    # CHECK STREAMS
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert len(from_stream) == 1


def test_basic_renew_with_multi_keys():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = '''test_basic_renew_with_multi_keys_app'''    
    TEST_INDEX_NAME = '''test_basic_renew_with_multi_keys_index'''
    TEST_INDEX_PREFIX = '''test_basic_renew_with_multi_keys_prefix:'''
    cct_prepare.create_index_with_prefix(producer, TEST_INDEX_PREFIX, TEST_INDEX_NAME)

    # ADD INITIAL DATA
    passport_value = "test_basic_renew_with_multi_keys_passport"
    for i in range(0, 3):
        d = cct_prepare.generate_single_object(1000 + i , 1000 - i, passport_value)
        key = TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)
    
    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1) # DEBUG QUERY TTL SET TO 1 second
    res = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    assert TEST_INDEX_PREFIX + str(0) in str(res)
    assert TEST_INDEX_PREFIX + str(1) in str(res)
    assert TEST_INDEX_PREFIX + str(2) in str(res)

    meta_data_query_key_1 = '''CCT2:QC:test_basic_renew_with_multi_keys_index:User\\.PASSPORT:test_basic_renew_with_multi_keys_passport:test_basic_renew_with_multi_keys_app'''
    assert producer.exists(meta_data_query_key_1) == 1

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    query_and_index_normalized_1 = '''test_basic_renew_with_multi_keys_index:User\\.PASSPORT:test_basic_renew_with_multi_keys_passport'''

    # RENEW
    client1.execute_command("CCT2.FT.RENEW " + query_and_index_normalized_1)

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    assert producer.exists(meta_data_query_key_1) == 1

    # TRIM STREAMS
    client1.xtrim(TEST_APP_NAME_1 , 0)

    # UPDATE DATA 0 
    d = cct_prepare.generate_single_object(0  , 0, passport_value)
    producer.json().set(TEST_INDEX_PREFIX + str(0), Path.root_path(), d)

    # CHECK STREAMS
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert '''test_basic_renew_with_multi_keys_prefix:0''' in str(from_stream[0])

    # RENEW
    client1.execute_command("CCT2.FT.RENEW " + query_and_index_normalized_1)

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    assert producer.exists(meta_data_query_key_1) == 1

    # TRIM STREAMS
    client1.xtrim(TEST_APP_NAME_1 , 0)

    # UPDATE DATA 1 
    d = cct_prepare.generate_single_object(1  , 1, passport_value)
    producer.json().set(TEST_INDEX_PREFIX + str(1), Path.root_path(), d)

    # CHECK STREAMS
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert '''test_basic_renew_with_multi_keys_prefix:1''' in str(from_stream[0])

    # RENEW
    client1.execute_command("CCT2.FT.RENEW " + query_and_index_normalized_1)

    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")

    assert producer.exists(meta_data_query_key_1) == 1

    # TRIM STREAMS
    client1.xtrim(TEST_APP_NAME_1 , 0)

    # UPDATE DATA 1 
    d = cct_prepare.generate_single_object(1  , 1, passport_value)
    producer.json().set(TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    # CHECK STREAMS
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert '''test_basic_renew_with_multi_keys_prefix:2''' in str(from_stream[0])





