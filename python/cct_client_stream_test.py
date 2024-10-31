import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_QUERIES, CCT_EOS


@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

def test_client_get_update_while_connected():
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

    # UPDATE DATA (K1)
    d = cct_prepare.generate_single_object(1000 , 2000, "ddd")
    producer.json().set(key_1, Path.root_path(), d)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 2 == len(from_stream[0][1])
    #print(from_stream)

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1001 , 2001, "ddd")
    producer.json().set(key_2, Path.root_path(), d)

    # CHECK STREAM
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 3 == len(from_stream[0][1])
    assert not from_stream[0][1][1][1][CCT_QUERIES]


def test_client_get_update_while_disconnected():
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
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") # match first two item
    first_query_normalized = "@User\\.PASSPORT:{aaa}"

    # DISCONNECT
    client1.connection_pool.disconnect()

    # UPDATE DATA (K1)
    d = cct_prepare.generate_single_object(1000 , 2000, "ddd")
    producer.json().set(key_1, Path.root_path(), d)

    # Check stream is empty
    from_stream = producer.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # RE-CONNECT
    client1 = connect_redis()

    # Check stream is still empty
    from_stream = producer.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream
    
    # REGISTER
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # Check stream 
    from_stream = producer.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert 2 == len(from_stream[0][1])
    assert first_query_normalized in from_stream[0][1][0][1][CCT_QUERIES]

    
