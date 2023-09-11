import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_MODULE_TRACKING_PREFIX, CCT_MODULE_QUERY_PREFIX, CCT_MODULE_CLIENT_QUERY_PREFIX

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
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d) # This is tracked by both client
    d = cct_prepare.generate_single_object(1001 , 2002, "bbb")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(3), Path.root_path(), d)       

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # PASS TIME
    time.sleep(6)

    # SECOND CLIENT
    query_value = 1001
    client2 = connect_redis()
    client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # CHECK TRACKED KEYS
    result = producer.sismember(CCT_MODULE_TRACKING_PREFIX +  cct_prepare.TEST_INDEX_PREFIX + str(1) ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_MODULE_TRACKING_PREFIX +  cct_prepare.TEST_INDEX_PREFIX + str(2) ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_MODULE_TRACKING_PREFIX +  cct_prepare.TEST_INDEX_PREFIX + str(3) ,  cct_prepare.TEST_APP_NAME_1)
    assert not result    

    result = producer.sismember(CCT_MODULE_TRACKING_PREFIX +  cct_prepare.TEST_INDEX_PREFIX + str(1) ,  cct_prepare.TEST_APP_NAME_2)
    assert not result   
    result = producer.sismember(CCT_MODULE_TRACKING_PREFIX +  cct_prepare.TEST_INDEX_PREFIX + str(2) ,  cct_prepare.TEST_APP_NAME_2)
    assert result
    result = producer.sismember(CCT_MODULE_TRACKING_PREFIX +  cct_prepare.TEST_INDEX_PREFIX + str(3) ,  cct_prepare.TEST_APP_NAME_2)
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
    result = producer.get(CCT_MODULE_CLIENT_QUERY_PREFIX + "User\\.PASSPORT:aaa:" + cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.get(CCT_MODULE_CLIENT_QUERY_PREFIX + "User\\.ID:1001:" + cct_prepare.TEST_APP_NAME_2)
    assert result  

    result = producer.sismember(CCT_MODULE_QUERY_PREFIX +  "User\\.PASSPORT:aaa" ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_MODULE_QUERY_PREFIX +"User\\.ID:1001" , cct_prepare.TEST_APP_NAME_2)
    assert result  

    # PASS TIME (Q1 expires after this)
    time.sleep(6)

    # CHECK EXPIRE Q1
    result = producer.get(CCT_MODULE_CLIENT_QUERY_PREFIX + "User\\.PASSPORT:aaa:" + cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.get(CCT_MODULE_CLIENT_QUERY_PREFIX + "User\\.ID:1001:" + cct_prepare.TEST_APP_NAME_2)
    assert result

    result = producer.sismember(CCT_MODULE_QUERY_PREFIX +  "User\\.PASSPORT:aaa" ,  cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.sismember(CCT_MODULE_QUERY_PREFIX +"User\\.ID:1001" , cct_prepare.TEST_APP_NAME_2)
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
    time.sleep(6)

    # CHECK EXPIRE Q2
    result = producer.get(CCT_MODULE_CLIENT_QUERY_PREFIX + "User\\.PASSPORT:aaa:" + cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.get(CCT_MODULE_CLIENT_QUERY_PREFIX + "User\\.ID:1001:" + cct_prepare.TEST_APP_NAME_2)
    assert not result  

    result = producer.sismember(CCT_MODULE_QUERY_PREFIX +  "User\\.PASSPORT:aaa" ,  cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.sismember(CCT_MODULE_QUERY_PREFIX +"User\\.ID:1001" , cct_prepare.TEST_APP_NAME_2)
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