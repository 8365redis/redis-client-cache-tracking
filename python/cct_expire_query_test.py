import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_MODULE_KEY_2_CLIENT, CCT_MODULE_QUERY_2_CLIENT, CCT_MODULE_QUERY_CLIENT, CCT_EXPIRE_HALF_TIME

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_query_expired():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d) # This is tracked by both client
    d = cct_prepare.generate_single_object(1001 , 2002, "bbb")
    key_3 = cct_prepare.TEST_INDEX_PREFIX + str(3) 
    producer.json().set(key_3, Path.root_path(), d)       

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # PASS TIME
    time.sleep(CCT_EXPIRE_HALF_TIME)

    # SECOND CLIENT
    query_value = 1001
    client2 = connect_redis()
    client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # CHECK TRACKED KEYS
    result = producer.sismember(CCT_MODULE_KEY_2_CLIENT +  cct_prepare.TEST_INDEX_PREFIX + str(1) ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_MODULE_KEY_2_CLIENT +  cct_prepare.TEST_INDEX_PREFIX + str(2) ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_MODULE_KEY_2_CLIENT +  cct_prepare.TEST_INDEX_PREFIX + str(3) ,  cct_prepare.TEST_APP_NAME_1)
    assert not result    

    result = producer.sismember(CCT_MODULE_KEY_2_CLIENT +  cct_prepare.TEST_INDEX_PREFIX + str(1) ,  cct_prepare.TEST_APP_NAME_2)
    assert not result   
    result = producer.sismember(CCT_MODULE_KEY_2_CLIENT +  cct_prepare.TEST_INDEX_PREFIX + str(2) ,  cct_prepare.TEST_APP_NAME_2)
    assert result
    result = producer.sismember(CCT_MODULE_KEY_2_CLIENT +  cct_prepare.TEST_INDEX_PREFIX + str(3) ,  cct_prepare.TEST_APP_NAME_2)
    assert result    

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2001, "aaa")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    print("#########STREAMS AFTER NON EXPIRED K2 UPDATED ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    print(from_stream)

    # CHECK BEFORE EXPIRE
    result = producer.exists(CCT_MODULE_QUERY_CLIENT + "User\\.PASSPORT:aaa:" + cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.exists(CCT_MODULE_QUERY_CLIENT + "User\\.ID:1001:" + cct_prepare.TEST_APP_NAME_2)
    assert result  

    result = producer.sismember(CCT_MODULE_QUERY_2_CLIENT +  "User\\.PASSPORT:aaa" ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_MODULE_QUERY_2_CLIENT +"User\\.ID:1001" , cct_prepare.TEST_APP_NAME_2)
    assert result  

    # PASS TIME (Q1 expires after this)
    time.sleep(CCT_EXPIRE_HALF_TIME)

    # CHECK EXPIRE Q1
    result = producer.exists(CCT_MODULE_QUERY_CLIENT + "User\\.PASSPORT:aaa:" + cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.exists(CCT_MODULE_QUERY_CLIENT + "User\\.ID:1001:" + cct_prepare.TEST_APP_NAME_2)
    assert result

    result = producer.sismember(CCT_MODULE_QUERY_2_CLIENT +  "User\\.PASSPORT:aaa" ,  cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.sismember(CCT_MODULE_QUERY_2_CLIENT +"User\\.ID:1001" , cct_prepare.TEST_APP_NAME_2)
    assert result

    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(1))
    assert not result
    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(2))
    assert result
    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(3))
    assert result


    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "ccc")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    print("#########STREAMS AFTER Q1 EXPIRE K2 UPDATED ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    print(from_stream)

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "ddd")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    print("#########STREAMS AFTER Q1 EXPIRE K2 UPDATED AGAIN ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    print(from_stream)

    # PASS TIME (Q2 expires after this)
    time.sleep(CCT_EXPIRE_HALF_TIME)

    # CHECK EXPIRE Q2
    result = producer.exists(CCT_MODULE_QUERY_CLIENT + "User\\.PASSPORT:aaa:" + cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.exists(CCT_MODULE_QUERY_CLIENT + "User\\.ID:1001:" + cct_prepare.TEST_APP_NAME_2)
    assert not result  

    result = producer.sismember(CCT_MODULE_QUERY_2_CLIENT +  "User\\.PASSPORT:aaa" ,  cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.sismember(CCT_MODULE_QUERY_2_CLIENT +"User\\.ID:1001" , cct_prepare.TEST_APP_NAME_2)
    assert not result  

    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(1))
    assert not result
    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(2)) # this will not expire because its key is updated so we are keep tracking
    assert result
    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(3))
    assert not result    

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "eee")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    print("#########STREAMS AFTER Q1&Q2 EXPIRE K2 UPDATED ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    print(from_stream)    

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "fff")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    print("#########STREAMS AFTER Q1&Q2 EXPIRE K2 UPDATED AGAIN############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    print(from_stream)        


    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(1))
    assert not result
    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(2)) # this will expire this time because even the related query is deleted and second one will delete this
    assert not result
    result = producer.exists(CCT_MODULE_KEY_2_CLIENT + cct_prepare.TEST_INDEX_PREFIX + str(3))
    assert not result

    
