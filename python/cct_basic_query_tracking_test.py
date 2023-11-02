import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_basic_query_tracking_test_1():
    r = connect_redis()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(1))

    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    print(resp)

    query_key_attr = "User\\.PASSPORT" + ":" + d["User"]["PASSPORT"]
    print("query_key_attr:" + query_key_attr)
    # SEARCH
    resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + d["User"]["PASSPORT"] + "}")
    assert resp
    print("CCT.FT.SEARCH Resp:" + str(resp))

    #CHECK TRACKED QUERY
    tracked_query = r.sismember(CCT_Q2C + query_key_attr, cct_prepare.TEST_APP_NAME_1)
    assert tracked_query

    #CHECK TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(1), cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 

    # SAME CLIENT ADDS A NEW DATA THAT MATCHES TO QUERY
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    new_key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    r.json().set(new_key, Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(2))

    # CHECK THE STREAM
    from_stream = r.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert new_key in str(from_stream[0][1])
    print("From Stream :" + str(from_stream[0][1]))

    #CHECK NEW TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(2), cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    


def test_basic_query_tracking_test_2():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    ####### FIRST CLIENT
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    first_key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    r.json().set(first_key, Path.root_path(), d)
    r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    ####### SECOND CLIENT
    r2 = connect_redis()
    r2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    r2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    
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

    ####### FIRST CLIENT
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    first_key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    r.json().set(first_key, Path.root_path(), d)
    r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    ####### SECOND CLIENT
    r2 = connect_redis()
    r2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    r2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    
    ####### THIRD CLIENT
    r3 = connect_redis()
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    new_added_key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    r3.json().set(new_added_key, Path.root_path(), d)

    r3.delete(new_added_key)

    # TESTS

    # CHECK THE STREAMS
    from_stream = r.xread( count=3, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert new_added_key in str(from_stream[0][1][-1][1])
    #assert from_stream[0][1][-1][1][new_added_key] == ''
    from_stream = r.xread( count=2, streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert new_added_key in str(from_stream[0][1][-1][1])
    #assert from_stream[0][1][-1][1][new_added_key] == '' 

    #CHECK NEW TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + first_key, cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 
    tracked_key = r.sismember(CCT_K2C + new_added_key, cct_prepare.TEST_APP_NAME_1)
    assert not tracked_key 
    tracked_key = r.sismember(CCT_K2C + first_key, cct_prepare.TEST_APP_NAME_2)
    assert tracked_key 
    tracked_key = r.sismember(CCT_K2C + new_added_key, cct_prepare.TEST_APP_NAME_2)
    assert not tracked_key     



