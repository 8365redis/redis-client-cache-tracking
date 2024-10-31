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
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") # match first two item

    # PASS TIME
    time.sleep(CCT_QUERY_HALF_TTL)

    # SECOND CLIENT
    query_value = 1001
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}") # match last two item

    # CHECK TRACKED KEYS
    result = producer.sismember(CCT_K2C +  key_1 ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_K2C +  key_2 ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_K2C +  key_3 ,  cct_prepare.TEST_APP_NAME_1)
    assert not result    

    result = producer.sismember(CCT_K2C + key_1 ,  cct_prepare.TEST_APP_NAME_2)
    assert not result   
    result = producer.sismember(CCT_K2C +  key_2 ,  cct_prepare.TEST_APP_NAME_2)
    assert result
    result = producer.sismember(CCT_K2C +  key_3 ,  cct_prepare.TEST_APP_NAME_2)
    assert result    

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2001, "aaa")
    producer.json().set(key_2, Path.root_path(), d)

    #print("#########STREAMS AFTER NON EXPIRED K2 UPDATED ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    ##print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    #print(from_stream)

    query_1 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa"
    query_2 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.ID:1001"

    # CHECK BEFORE EXPIRE
    result = producer.exists(CCT_QC + query_1 + CCT_DELI + cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.exists(CCT_QC + query_2 + CCT_DELI + cct_prepare.TEST_APP_NAME_2)
    assert result

    result = producer.sismember(CCT_Q2C +  query_1 ,  cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.sismember(CCT_Q2C + query_2 , cct_prepare.TEST_APP_NAME_2)
    assert result

    # PASS TIME (Q1 expires after this)
    time.sleep(CCT_QUERY_HALF_TTL)

    # CHECK EXPIRE Q1
    result = producer.exists(CCT_QC + query_1 + CCT_DELI + cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.exists(CCT_QC + query_2 + CCT_DELI + cct_prepare.TEST_APP_NAME_2)
    assert result

    result = producer.sismember(CCT_Q2C + query_1 ,  cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.sismember(CCT_Q2C + query_2 , cct_prepare.TEST_APP_NAME_2)
    assert result

    result = producer.exists(CCT_K2C + key_1)
    assert not result
    result = producer.exists(CCT_K2C + key_2)
    assert result
    result = producer.exists(CCT_K2C + key_3)
    assert result


    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "ccc")
    producer.json().set(key_2, Path.root_path(), d)

    #print("#########STREAMS AFTER Q1 EXPIRE K2 UPDATED ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    #print(from_stream)

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "ddd")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    #print("#########STREAMS AFTER Q1 EXPIRE K2 UPDATED AGAIN ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    #print(from_stream)

    # PASS TIME (Q2 expires after this)
    time.sleep(CCT_QUERY_HALF_TTL)

    # CHECK EXPIRE Q2
    result = producer.exists(CCT_QC + query_1 + cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.exists(CCT_QC + query_2 + cct_prepare.TEST_APP_NAME_2)
    assert not result  

    result = producer.sismember(CCT_Q2C + query_1 ,  cct_prepare.TEST_APP_NAME_1)
    assert not result
    result = producer.sismember(CCT_Q2C + query_2 , cct_prepare.TEST_APP_NAME_2)
    assert not result  

    result = producer.exists(CCT_K2C + key_1)
    assert not result
    result = producer.exists(CCT_K2C + key_2) # this will not expire because its key is updated so we are keep tracking
    assert result
    result = producer.exists(CCT_K2C + key_3)
    assert not result    

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "eee")
    producer.json().set(key_2, Path.root_path(), d)

    #print("#########STREAMS AFTER Q1&Q2 EXPIRE K2 UPDATED ############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    #print(from_stream)    

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2000, "fff")
    producer.json().set(key_2, Path.root_path(), d)

    #print("#########STREAMS AFTER Q1&Q2 EXPIRE K2 UPDATED AGAIN############")

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(from_stream)
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    #print(from_stream)        


    result = producer.exists(CCT_K2C + key_1)
    assert not result
    result = producer.exists(CCT_K2C + key_2) # this will expire this time because even the related query is deleted and second one will delete this
    assert not result
    result = producer.exists(CCT_K2C + key_3)
    assert not result

    
def test_1_client_1_query_1_key_expired():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa"
    # CHECK CCT_META_DATA 
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, query_normalized, key_1, [True]*6 )

    time.sleep(CCT_QUERY_TTL + 1)

    # CHECK CCT_META_DATA 
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, query_normalized, key_1, [False]*6 )

def test_1_client_2_query_1_key_expired():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)

    # FIRST CLIENT FiRST QUERY
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    time.sleep(CCT_QUERY_HALF_TTL)

    #FIRST CLIENT SECOND QUERY
    query_value = 1000
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    first_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa"
    second_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.ID:1000"

    # CHECK CCT_META_DATA 

    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [True]*6 )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, second_query_normalized, key_1, [True]*6 )     

    # THIS WILL EXPIRE FIRST QUERY
    time.sleep(CCT_QUERY_HALF_TTL)

    # CHECK CCT_META_DATA 

    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False] + [True] + [False]*4  )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, second_query_normalized, key_1, [True]*6 )

    # THIS WILL EXPIRE SECOND QUERY
    time.sleep(CCT_QUERY_HALF_TTL)

    # CHECK CCT_META_DATA 

    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, second_query_normalized, key_1, [False]*6 )

# 1 client makes 2 queries matches to different 2 key
def test_1_client_2_query_2_key_expired():
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
    producer.json().set(key_2, Path.root_path(), d)

    # FIRST CLIENT FiRST QUERY
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    time.sleep(CCT_QUERY_HALF_TTL)

    #FIRST CLIENT SECOND QUERY
    query_value = 1001
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    first_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa"
    second_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.ID:1001"

    # CHECK CCT_META_DATA

    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [True]*6 )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, second_query_normalized, key_2, [True]*6 ) 

    # THIS WILL EXPIRE FIRST QUERY
    time.sleep(CCT_QUERY_HALF_TTL)

    # CHECK CCT_META_DATA 
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False]*6 )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, second_query_normalized, key_2, [True]*6 )

    # THIS WILL EXPIRE SECOND QUERY
    time.sleep(CCT_QUERY_HALF_TTL)

    # CHECK CCT_META_DATA 
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, second_query_normalized, key_2, [False]*6 )

# 2 client makes 2 same query matches to same 1 key
def test_2_client_1_query_1_key_expired_same_time():
    
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)

    first_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa"

    # FIRST CLIENT FiRST QUERY
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + query_value + "}")

    #SECOND CLIENT SAME QUERY
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + query_value + "}")

    # CHECK CCT_META_DATA
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [True]*6 )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_2, first_query_normalized, key_1, [True]*6 ) 

    # THIS WILL EXPIRE BOTH QUERIES
    time.sleep(CCT_QUERY_TTL+1)

    # CHECK CCT_META_DATA
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False]*6 )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_2, first_query_normalized, key_1, [False]*6 )

# 2 client makes 2 same query matches to same 1 key
def test_2_client_1_query_1_key_expired_sequentially():
    
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)

    first_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa"

    # FIRST CLIENT FiRST QUERY
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + query_value + "}")

    # WAIT
    time.sleep(CCT_QUERY_HALF_TTL)

    # SECOND CLIENT SAME QUERY
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + query_value + "}")

    # CHECK CCT_META_DATA
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [True]*6 )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_2, first_query_normalized, key_1, [True]*6 ) 

    # THIS WILL EXPIRE FIRST QUERY
    time.sleep(CCT_QUERY_HALF_TTL)
    
    # CHECK CCT_META_DATA
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False,True,True,False,True,False]  )   
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_2, first_query_normalized, key_1, [True]*6 )

    # THIS WILL EXPIRE SECOND QUERY
    time.sleep(CCT_QUERY_HALF_TTL)
    
    # CHECK CCT_META_DATA
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False]*6  )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_2, first_query_normalized, key_1, [False]*6 )


# 2 client makes 2 different query matches to same 1 key
def test_2_client_2_query_1_key_expired_same_time():
    
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)

    first_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa"
    second_query_normalized = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.ID:1000"

    # FIRST CLIENT FiRST QUERY
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.PASSPORT:{" + query_value + "}")

    #SECOND CLIENT SECOND QUERY
    query_value = 1000
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " @User\\.ID:{" + str(query_value) + "}")

    # CHECK CCT_META_DATA
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [True]*6)
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_2, second_query_normalized, key_1, [True]*6) 

    # THIS WILL EXPIRE BOTH QUERIES
    time.sleep(CCT_QUERY_TTL+1)

    # CHECK CCT_META_DATA
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False]*6 )
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_2, second_query_normalized, key_1, [False]*6 )