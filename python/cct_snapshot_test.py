import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC, \
                CCT_HALF_TTL, CCT_TTL, CCT_MODULE_PREFIX
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
    assert not from_stream

    # DISCONNECT
    client1.close()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1]["queries"]

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
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # DISCONNECT
    client1.close()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert first_query_normalized in from_stream[0][1][0][1]["queries"]
    assert second_query_normalized in from_stream[0][1][0][1]["queries"]

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
    assert not from_stream

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1]["queries"]

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
    assert not from_stream

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert first_query_normalized in from_stream[0][1][0][1]["queries"]
    assert second_query_normalized in from_stream[0][1][0][1]["queries"]


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
    assert 3 == len(from_stream[0][1])

    # DISCONNECT
    client1.close()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1]["queries"]
    assert 1 == len(from_stream[0][1])

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
    assert 3 == len(from_stream[0][1])

    # DISCONNECT
    client1.close()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_1 in str(from_stream[0][1][0][1])
    assert query_normalized in from_stream[0][1][0][1]["queries"]
    assert 1 == len(from_stream[0][1])

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
    assert 4 == len(from_stream[0][1])

    # DISCONNECT
    client1.close()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_2 in str(from_stream[0][1][0][1])
    assert key_1 in str(from_stream[0][1][1][1])
    assert query_normalized in from_stream[0][1][0][1]["queries"]
    assert str(2006) in from_stream[0][1][0][1]["value"]
    assert 2 == len(from_stream[0][1])

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
    assert 6 == len(from_stream[0][1])

    # DISCONNECT
    client1.close()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream content
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert cct_prepare.TEST_APP_NAME_1 in from_stream[0][0]
    assert key_2 in str(from_stream[0][1][0][1])
    assert key_1 in str(from_stream[0][1][1][1])
    assert query_normalized in from_stream[0][1][0][1]["queries"]
    assert query_normalized in from_stream[0][1][1][1]["queries"]
    assert 2 == len(from_stream[0][1])

def test_1_client_multiple_query_multiple_key_multiple_update_mixed_match_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)