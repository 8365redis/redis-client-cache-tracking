import pytest
import time
import redis
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, SKIP_HB_TEST,  \
                CCT_EOS, CCT_QUERY_TTL

import constants
from cct_test_utils import get_redis_snapshot

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_heartbeat_trims_one_from_stream():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)    

    client1 = connect_redis()
    res = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    query_normalized = "User\\.PASSPORT:aaa"

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    read_ids = []
    assert 3 == len(from_stream[0][1])
    for id, value in from_stream[0][1]:
        read_ids.append(id)
        #print( f"id: {id} value: {value}")
    
    # SEND HB with trim
    max_id = max(read_ids)
    res = client1.execute_command("CCT2.HEARTBEAT " + str(max_id))
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # UPDATE DATA AGAIN
    d = cct_prepare.generate_single_object(1003 , 2003 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1004 , 2004 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    read_ids = []
    assert 2 == len(from_stream[0][1])
    for id, value in from_stream[0][1]:
        read_ids.append(id)
    
    # SEND HB without trim
    res = client1.execute_command("CCT2.HEARTBEAT" )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 2 == len(from_stream[0][1])

    # SEND HB with trim
    min_id = min(read_ids)
    res = client1.execute_command("CCT2.HEARTBEAT " + str(min_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream[0][1]) 

    # SEND HB with trim
    max_id = max(read_ids)
    res = client1.execute_command("CCT2.HEARTBEAT " + str(max_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # DISCONNECT CLIENT
    client1.connection_pool.disconnect()
    time.sleep(1.1)

@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_heartbeat_trims_stream_with_invalid_ids():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)    

    client1 = connect_redis()
    res = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    read_ids = []
    assert 3 == len(from_stream[0][1])
    for id, value in from_stream[0][1]:
        read_ids.append(id)
    
    # SEND HB with trim lower than min
    min_id = min(read_ids)
    #print('Normal min id  :' + str(min_id) )
    new_min_id = min_id.split('-')[0]
    new_min_id = new_min_id[:-4] + "0000"
    new_min_id = new_min_id + "-0"
    #print('New min id     :' + str(new_min_id) )
    res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])

    # SEND HB with trim invalid value
    min_id = min(read_ids)
    #print('Normal min id  :' + str(min_id) )
    new_min_id = "234324234"
    #print('New min id     :' + str(new_min_id) )
    res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])

    # SEND HB with trim invalid value
    new_min_id = "234324234-2"
    #print('New min id     :' + str(new_min_id) )
    res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1]) 

    # SEND HB with trim invalid value
    new_min_id = "-121212212"
    #print('New min id     :' + str(new_min_id) )
    try:
        res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    except redis.exceptions.ResponseError as e:
        assert "failed" in str(e)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])

    # SEND HB with trim invalid value
    new_min_id = "-121212212-1212"
    #print('New min id     :' + str(new_min_id) )
    try:
        res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    except redis.exceptions.ResponseError as e:
        assert "failed" in str(e)

    
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])

    # SEND HB with trim invalid value
    new_min_id = "asdasdasdsasd"
    #print('New min id     :' + str(new_min_id) )
    try:
        res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    except redis.exceptions.ResponseError as e:
        assert "failed" in str(e)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])  

    # SEND HB with trim invalid value
    new_min_id = "asdasdasdsasd-2"
    #print('New min id     :' + str(new_min_id) )
    try:
        res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    except redis.exceptions.ResponseError as e:
        assert "failed" in str(e)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])  

    # SEND HB with trim higher than max
    max_id = max(read_ids)
    #print('Normal max id  :' + str(max_id) )
    new_min_id = min_id.split('-')[0]
    new_min_id = new_min_id[:-6] + "999999"
    new_min_id = new_min_id + "-999"
    #print('New max id     :' + str(new_min_id) )
    res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # DISCONNECT CLIENT
    client1.connection_pool.disconnect()


@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_heartbeat_trims_empty():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    client1 = connect_redis()
    res = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # SEND HB with trim invalid value
    new_min_id = "234324234"
    #print('New min id     :' + str(new_min_id) )
    res = client1.execute_command("CCT2.HEARTBEAT " + str(new_min_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # DISCONNECT CLIENT
    client1.connection_pool.disconnect()

    #time.sleep(4.1)


@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_heartbeat_trims_stream_after_snapshot():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2000, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1003 , 2000, passport_value)
    key_3 = cct_prepare.TEST_INDEX_PREFIX + str(3) 
    producer.json().set(key_3, Path.root_path(), d)

    client1 = connect_redis()
    res = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)

    # FIRST CLIENT
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1000 , 2002 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2003 ,passport_value)
    producer.json().set(key_2, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2004 ,passport_value)
    producer.json().set(key_2, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2005 ,passport_value)
    producer.json().set(key_3, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2006 ,passport_value)
    producer.json().set(key_3, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    read_ids = []
    assert 7 == len(from_stream[0][1])
    for id, value in from_stream[0][1]:
        read_ids.append(id)

    # DISCONNECT CLIENT
    client1.connection_pool.disconnect()

    time.sleep(1.1)

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    read_ids = []
    assert 4 == len(from_stream[0][1])
    for id, value in from_stream[0][1]:
        read_ids.append(id)
    
    # SEND HB with trim
    max_id = max(read_ids)
    res = client1.execute_command("CCT2.HEARTBEAT " + str(max_id) )
    assert cct_prepare.OK in str(res)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream
    
    # THIS WILL EXPIRE QUERY 
    time.sleep(CCT_QUERY_TTL + 1.1)

    # DISCONNECT CLIENT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert len(from_stream[0][1]) == 1

    client1.connection_pool.disconnect()