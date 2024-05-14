import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_QUERIES, CCT_VALUE, CCT_QUERY_HALF_TTL, CCT_QUERY_TTL, CCT_EOS
from cct_test_utils import check_query_meta_data

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_1_client_1_query_with_disconnect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # Check stream is empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(0.2)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(from_stream)
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1][CCT_QUERIES]

def test_1_client_2_query_with_disconnect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1000"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    #FIRST CLIENT SECOND QUERY
    query_value = 1000
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # Check stream is empty
    time.sleep(0.5)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert first_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert second_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]

def test_1_client_1_query_without_disconnect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # Check stream is empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1][CCT_QUERIES]

def test_1_client_2_query_without_disconnect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1000"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    #FIRST CLIENT SECOND QUERY
    query_value = 1000
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # Check stream is empty
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert first_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert second_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]


def test_1_client_1_query_1_key_multiple_update_still_match_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2001 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)    

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 4 == len(from_stream[0][1])

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    time.sleep(0.1)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert 2 == len(from_stream[0][1])

def test_1_client_1_query_1_key_multiple_update_doesnt_match_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2001 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 , "bbb")
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 , "ccc")
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1003 , 2003 , "ddd")
    producer.json().set(key_1, Path.root_path(), d)    

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 4 == len(from_stream[0][1])

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert 2 == len(from_stream[0][1])

def test_1_client_1_query_multiple_key_multiple_update_still_match_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1000 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)    
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    d = cct_prepare.generate_single_object(1005 , 2000 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1006 , 2000 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1000 , 2005 , passport_value)
    producer.json().set(key_2, Path.root_path(), d) 
    d = cct_prepare.generate_single_object(1000 , 2006 , passport_value)
    producer.json().set(key_2, Path.root_path(), d)     

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 5 == len(from_stream[0][1])

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    time.sleep(0.1)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_2 in str(from_stream[0][1][0][1])
    assert key_1 in str(from_stream[0][1][1][1])
    assert query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert str(2006) in from_stream[0][1][0][1][CCT_VALUE]
    assert 3 == len(from_stream[0][1])

def test_1_client_1_query_multiple_key_multiple_update_doesnt_match_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1000 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)    
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    d = cct_prepare.generate_single_object(1001 , 2001 ,passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1007 , 2007 , passport_value)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 , "bbb")
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 , "ccc")
    producer.json().set(key_1, Path.root_path(), d)

    d = cct_prepare.generate_single_object(1004 , 2004 , "ddd")
    producer.json().set(key_2, Path.root_path(), d)    
    d = cct_prepare.generate_single_object(1004 , 2004 , passport_value)
    producer.json().set(key_2, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1007 , 2007 , "ddd")
    producer.json().set(key_2, Path.root_path(), d)    

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 7 == len(from_stream[0][1])

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_2 in str(from_stream[0][1][0][1])
    assert key_1 in str(from_stream[0][1][1][1])
    assert query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert query_normalized in from_stream[0][1][1][1][CCT_QUERIES]
    assert 3 == len(from_stream[0][1])

def test_1_client_multiple_query_multiple_key_multiple_update_mixed_match_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    id_value = 1001
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)    
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1001"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    query_value = id_value
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2003 ,passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d) 
    d = cct_prepare.generate_single_object(1004 , 2004 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2005 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)

    d = cct_prepare.generate_single_object(1001 , 2004 , passport_value) # match both
    producer.json().set(key_2, Path.root_path(), d)    
    d = cct_prepare.generate_single_object(1001 , 2004 , "ccc") # match one 
    producer.json().set(key_2, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1007 , 2007 , "ddd") # match none
    producer.json().set(key_2, Path.root_path(), d)

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 7 == len(from_stream[0][1])

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert 3 == len(from_stream[0][1])
    assert key_2 in str(from_stream[0][1][0][1])
    assert key_1 in str(from_stream[0][1][1][1])
    assert first_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert second_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert first_query_normalized in from_stream[0][1][1][1][CCT_QUERIES]


def test_1_client_multiple_query_multiple_key_multiple_update_mixed_match_query_without_disconnect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    id_value = 1001
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)    
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1001"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    query_value = id_value
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2003 ,passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d) 
    d = cct_prepare.generate_single_object(1004 , 2004 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2005 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)

    d = cct_prepare.generate_single_object(1001 , 2004 , passport_value) # match both
    producer.json().set(key_2, Path.root_path(), d)    
    d = cct_prepare.generate_single_object(1001 , 2004 , "ccc") # match one 
    producer.json().set(key_2, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1007 , 2007 , "ddd") # match none
    producer.json().set(key_2, Path.root_path(), d)

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 7 == len(from_stream[0][1])

    # RE-REGISTER
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert 3 == len(from_stream[0][1])
    assert key_2 in str(from_stream[0][1][0][1])
    assert key_1 in str(from_stream[0][1][1][1])
    assert first_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert second_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]
    assert first_query_normalized in from_stream[0][1][1][1][CCT_QUERIES]

def test_1_client_multiple_query_multiple_key_multiple_update_mixed_match_query_1_query_expire_while_disconnect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    id_value = 1001
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)    
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1001"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    
    time.sleep(CCT_QUERY_HALF_TTL)

    query_value = id_value
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2003 ,passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d) 
    d = cct_prepare.generate_single_object(1004 , 2004 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2005 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)

    d = cct_prepare.generate_single_object(1001 , 2004 , passport_value) # match both
    producer.json().set(key_2, Path.root_path(), d)    

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 5 == len(from_stream[0][1])

    # DISCONNECT
    client1.connection_pool.disconnect()

    # THIS WILL EXPIRE FIRST QUERY 
    time.sleep(CCT_QUERY_HALF_TTL)

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert 2 == len(from_stream[0][1])
    assert key_2 in str(from_stream[0][1][0][1])
    assert second_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]

def test_1_client_multiple_query_multiple_key_multiple_update_mixed_match_query_1_query_expire_while_connected():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    id_value = 1001
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)    
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1001"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    
    time.sleep(CCT_QUERY_HALF_TTL)

    query_value = id_value
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2003 ,passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d) 
    d = cct_prepare.generate_single_object(1004 , 2004 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2005 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)

    d = cct_prepare.generate_single_object(1001 , 2004 , passport_value) # match both
    producer.json().set(key_2, Path.root_path(), d)    

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 5 == len(from_stream[0][1])
    print(from_stream)

    # THIS WILL EXPIRE FIRST QUERY 
    time.sleep(CCT_QUERY_HALF_TTL)

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    time.sleep(0.1)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 2 == len(from_stream[0][1])
    assert key_2 in str(from_stream[0][1][0][1])
    assert second_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]


def test_1_client_multiple_query_multiple_key_multiple_update_mixed_match_query_all_query_expire():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    id_value = 1001
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    key_2 = cct_prepare.TEST_INDEX_PREFIX + str(2) 
    producer.json().set(key_2, Path.root_path(), d)    
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1001"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    
    time.sleep(CCT_QUERY_HALF_TTL)

    query_value = id_value
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2003 ,passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d) 
    d = cct_prepare.generate_single_object(1004 , 2004 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2005 , passport_value) # match one 
    producer.json().set(key_1, Path.root_path(), d)

    d = cct_prepare.generate_single_object(1001 , 2004 , passport_value) # match both
    producer.json().set(key_2, Path.root_path(), d)    

    # Check stream is not empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert 4 == len(from_stream[0][0])

    # THIS WILL EXPIRE FIRST QUERY 
    time.sleep(CCT_QUERY_HALF_TTL)

    # DISCONNECT
    client1.connection_pool.disconnect()

    # THIS WILL EXPIRE SECOND QUERY 
    time.sleep(CCT_QUERY_HALF_TTL)    

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream)

def test_1_client_1_query_key_expire():
    KEY_EXPIRE_SECOND = 1 # Expire before the query

    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    producer.expire(key_1, KEY_EXPIRE_SECOND, nx = True)
    passport_value = "bbb"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # Check stream is empty
    time.sleep(0.2)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # THIS WILL EXPIRE KEY 
    time.sleep(KEY_EXPIRE_SECOND + 0.1)   

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream)

def test_1_client_1_query_first_key_later_query_expire():
    KEY_EXPIRE_SECOND = 1 # Expire before the query

    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    producer.expire(key_1, KEY_EXPIRE_SECOND, nx = True)
    passport_value = "bbb"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # Check stream is empty
    time.sleep(0.1)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # THIS WILL EXPIRE KEY 
    time.sleep(KEY_EXPIRE_SECOND + 0.1)

    # THIS WILL QUERY 
    time.sleep(CCT_QUERY_TTL + 0.1)

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream)

def test_1_client_1_query_first_query_later_key_expire():
    KEY_EXPIRE_SECOND = 1 # Expire before the query

    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    producer.expire(key_1, (CCT_QUERY_TTL+1), nx = True)
    passport_value = "bbb"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # Check stream is empty
    time.sleep(1)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # THIS WILL EXPIRE KEY AND QUERY 
    time.sleep(CCT_QUERY_TTL + 1)  

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 1 == len(from_stream)

def test_empty_queries_in_snapshot():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "User\\.PASSPORT:aaa"

    # FIRST CLIENT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    query_value = "non_existing_1"
    query_normalized_non_matching_1 = "User\\.PASSPORT:non_existing_1"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") # This will not match
    query_value = "non_existing_2"
    query_normalized_non_matching_2 = "User\\.PASSPORT:non_existing_2"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") # This will not match

    # Check stream is empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(0.2)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1][CCT_QUERIES]

def test_2_client_2_query_with_matching_queries():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value_1 = "pass1"
    id_value_1 = "id1"
    passport_value_2 = "pass2"
    id_value_2 = "id2"
    d = cct_prepare.generate_single_object(id_value_1 , 2000, passport_value_1)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1) 
    producer.json().set(key_1, Path.root_path(), d)
    query_normalized = "@User\\.PASSPORT:{aaa}"

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value_1 + "}") # RIC == PASSPORT
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(id_value_1) + "}") # MLP_ID == ID

    # UPDATE KEYS
    d = cct_prepare.generate_single_object(id_value_1 , 2000, passport_value_2)
    producer.json().set(key_1, Path.root_path(), d)
    d = cct_prepare.generate_single_object(id_value_2 , 2000, passport_value_1)
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream is empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # DISCONNECT CLIENT1
    client1.connection_pool.disconnect()

    # CONNECT CLIENT 2
    client2 = connect_redis()
    client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value_1 + "}")
    client2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(id_value_2) + "}")
    time.sleep(0.2)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    print(from_stream)
    assert cct_prepare.TEST_APP_NAME_2 in from_stream[0][0]
