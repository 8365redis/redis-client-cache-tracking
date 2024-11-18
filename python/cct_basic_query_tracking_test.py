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

def test_basic_query_tracking_test_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(1))

    # REGISTER
    resp = r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    #print(resp)

    query_key_attr = "User\\.PASSPORT" + ":" + d["User"]["PASSPORT"]
    #print("query_key_attr:" + query_key_attr)
    # SEARCH
    resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + d["User"]["PASSPORT"] + "}")
    assert resp
    #print("CCT2.FT.SEARCH Resp:" + str(resp))

    #print(get_redis_snapshot())

    #CHECK TRACKED QUERY
    tracked_query = r.sismember(CCT_Q2C +  cct_prepare.TEST_INDEX_NAME + CCT_DELI +query_key_attr, cct_prepare.TEST_APP_NAME_1)
    assert tracked_query

    #CHECK TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(1), cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 

    # SAME CLIENT ADDS A NEW DATA THAT MATCHES TO QUERY
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    new_key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    r.json().set(new_key, Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(2))

    time.sleep(0.1)

    # CHECK THE STREAM
    from_stream = r.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    #assert new_key in str(from_stream[0][1])
    #print("From Stream :" + str(from_stream[0][1]))

    #CHECK NEW TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(2), cct_prepare.TEST_APP_NAME_1)
    #assert tracked_key 

def test_basic_query_tracking_test_2():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    ####### FIRST CLIENT
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    first_key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    r.json().set(first_key, Path.root_path(), d)
    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    ####### SECOND CLIENT
    r2 = connect_redis()
    r2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    r2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    
    ####### THIRD CLIENT
    r3 = connect_redis()
    d = cct_prepare.generate_single_object(1001 , 2002, passport_value)
    new_added_key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    r3.json().set(new_added_key, Path.root_path(), d)

    # TESTS

    # CHECK THE STREAMS
    from_stream = r.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert new_added_key in str(from_stream[0][1])
    from_stream = r.xread( count=2, streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert new_added_key in str(from_stream[0][1])    

    #CHECK NEW TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + first_key, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = r.sismember(CCT_K2C + new_added_key, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = r.sismember(CCT_K2C + first_key, cct_prepare.TEST_APP_NAME_2)
    assert tracked_key 
    tracked_key = r.sismember(CCT_K2C + new_added_key, cct_prepare.TEST_APP_NAME_2)
    assert tracked_key     

def test_basic_query_tracking_test_3():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    TEST_APP_NAME_1 = "test_app_1"
    TEST_APP_NAME_2 = "test_app_2"

    ####### FIRST CLIENT
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    first_key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    r.json().set(first_key, Path.root_path(), d)
    r.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    ####### SECOND CLIENT
    r2 = connect_redis()
    r2.execute_command("CCT2.REGISTER " + TEST_APP_NAME_2)
    r2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    
    ####### THIRD CLIENT
    r3 = connect_redis()
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    new_added_key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    r3.json().set(new_added_key, Path.root_path(), d)

    r3.delete(new_added_key)

    # TESTS

    # CHECK THE STREAMS
    from_stream = r.xread( streams={TEST_APP_NAME_1:0} )
    assert new_added_key in str(from_stream[0][1][-1][1])
    #assert from_stream[0][1][-1][1][new_added_key] == ''
    from_stream = r.xread(streams={TEST_APP_NAME_2:0} )
    assert new_added_key in str(from_stream[0][1][-1][1])
    #assert from_stream[0][1][-1][1][new_added_key] == '' 

    #CHECK NEW TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + first_key, TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = r.sismember(CCT_K2C + new_added_key, TEST_APP_NAME_1)
    assert not tracked_key 
    tracked_key = r.sismember(CCT_K2C + first_key, TEST_APP_NAME_2)
    assert tracked_key 
    tracked_key = r.sismember(CCT_K2C + new_added_key, TEST_APP_NAME_2)
    assert not tracked_key     



def test_key_change_still_tracked():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first

    TEST_APP_NAME_1 = "test_key_change_still_tracked_app_1"
    TEST_INDEX_PREFIX  = "test_key_change_still_tracked_prefix:"
    TEST_INDEX = "test_key_change_still_tracked_index"
    
    cct_prepare.create_index_with_prefix(r,TEST_INDEX_PREFIX, TEST_INDEX)

    # ADD INITIAL DATA
    query_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    key = TEST_INDEX_PREFIX + str(1)
    r.json().set(key, Path.root_path(), d)
    assert r.json().get(key)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # SEARCH
    res = client1.execute_command("CCT2.FT.SEARCH " + TEST_INDEX + " @User\\.PASSPORT:{" + query_value + "}")
    assert str(res) == '''[1, 'test_key_change_still_tracked_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"aaa","Address":{"ID":"2000"}}}']]'''

    # UPDATE DATA
    d = cct_prepare.generate_single_object(9999 , 9999, query_value)
    r.json().set(key, Path.root_path(), d)
    assert r.json().get(key)
    
    time.sleep(0.1)

    # CHECK STREAM
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert '''test_key_change_still_tracked_prefix:1''' in str(from_stream)


def test_key_change_not_tracked():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first

    TEST_APP_NAME_1 = "test_key_change_not_tracked_app_1"
    TEST_INDEX_PREFIX  = "test_key_change_not_tracked_prefix:"
    TEST_INDEX = "test_key_change_not_tracked_index"
    
    cct_prepare.create_index_with_prefix(r,TEST_INDEX_PREFIX, TEST_INDEX)

    # ADD INITIAL DATA
    query_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    key = TEST_INDEX_PREFIX + str(1)
    r.json().set(key, Path.root_path(), d)
    assert r.json().get(key)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # SEARCH
    res = client1.execute_command("CCT2.FT.SEARCH " + TEST_INDEX + " @User\\.PASSPORT:{" + query_value + "}")
    assert str(res) == '''[1, 'test_key_change_not_tracked_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"aaa","Address":{"ID":"2000"}}}']]'''

    # UPDATE DATA
    passport_value = 'bbb'
    d = cct_prepare.generate_single_object(9999 , 9999, passport_value)
    r.json().set(key, Path.root_path(), d)
    assert r.json().get(key)
    
    time.sleep(0.1)

    # CHECK STREAM
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert '''test_key_change_not_tracked_prefix:1''' in str(from_stream)

    # SEARCH again
    res = client1.execute_command("CCT2.FT.SEARCH " + TEST_INDEX + " @User\\.PASSPORT:{" + query_value + "}")
    assert str(res) == '''[0]'''


def test_key_deleted():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first

    TEST_APP_NAME_1 = "test_key_deleted_app_1"
    TEST_INDEX_PREFIX  = "test_key_deleted_prefix:"
    TEST_INDEX = "test_key_deleted_index"
    
    cct_prepare.create_index_with_prefix(r,TEST_INDEX_PREFIX, TEST_INDEX)

    # ADD INITIAL DATA
    query_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    key = TEST_INDEX_PREFIX + str(1)
    r.json().set(key, Path.root_path(), d)
    assert r.json().get(key)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # SEARCH
    res = client1.execute_command("CCT2.FT.SEARCH " + TEST_INDEX + " @User\\.PASSPORT:{" + query_value + "}")
    assert str(res) == '''[1, 'test_key_deleted_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"aaa","Address":{"ID":"2000"}}}']]'''

    # UPDATE DATA
    r.delete(key)
    
    time.sleep(0.1)

    # CHECK STREAM
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert '''test_key_deleted_prefix:1''' in str(from_stream)
    assert '''DELETE''' in str(from_stream)

    # SEARCH again
    res = client1.execute_command("CCT2.FT.SEARCH " + TEST_INDEX + " @User\\.PASSPORT:{" + query_value + "}")
    assert str(res) == '''[0]'''


def test_key_expired():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first

    TEST_APP_NAME_1 = "test_key_expired_app_1"
    TEST_INDEX_PREFIX  = "test_key_expired_prefix:"
    TEST_INDEX = "test_key_expired_index"
    
    cct_prepare.create_index_with_prefix(r,TEST_INDEX_PREFIX, TEST_INDEX)

    # ADD INITIAL DATA
    query_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    key = TEST_INDEX_PREFIX + str(1)
    r.json().set(key, Path.root_path(), d)
    r.expire(key, 1, nx=True)
    assert r.json().get(key)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1 + " 1000")
    assert cct_prepare.OK in str(resp)

    # SEARCH
    res = client1.execute_command("CCT2.FT.SEARCH " + TEST_INDEX + " @User\\.PASSPORT:{" + query_value + "}")
    assert str(res) == '''[1, 'test_key_expired_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"aaa","Address":{"ID":"2000"}}}']]'''

    # EXPIRE
    time.sleep(0.6)
    client1.execute_command("CCT2.HEARTBEAT")  
    time.sleep(0.6)

    # CHECK STREAM
    from_stream = client1.xread(streams={TEST_APP_NAME_1:0} )
    assert '''EXPIRE''' in str(from_stream)

    # SEARCH again
    res = client1.execute_command("CCT2.FT.SEARCH " + TEST_INDEX + " @User\\.PASSPORT:{" + query_value + "}")
    assert str(res) == '''[0]'''
