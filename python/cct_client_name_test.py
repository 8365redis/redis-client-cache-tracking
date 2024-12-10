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

def test_register_with_client_name():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = 'test_search_with_client_name_app_1'
    TEST_APP_NAME_2 = 'test_search_with_client_name_app_2'
    TEST_APP_NAME_3 = 'test_search_with_client_name_app_3'
    TEST_APP_NAME_4 = 'test_search_with_client_name_app_4'
    TEST_APP_NAME_5 = 'test_search_with_client_name_app_5'
    TEST_APP_NAME_6 = 'test_search_with_client_name_app_6'
    TEST_APP_NAME_7 = 'test_search_with_client_name_app_7'

    TEST_APP_GROUP_NAME_3 = 'test_search_with_client_name_app_group_3'
    TEST_APP_GROUP_NAME_5 = 'test_search_with_client_name_app_group_5'
    TEST_APP_GROUP_NAME_6 = 'test_search_with_client_name_app_group_6'
    TEST_APP_GROUP_NAME_7 = 'test_search_with_client_name_app_group_7'

    client1 = connect_redis()
    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_1) == True

    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_2 + " CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_2) == True

    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_3 + " " + TEST_APP_GROUP_NAME_3 + " CLIENTNAME " + TEST_APP_NAME_3)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_3) == True

    res = client1.execute_command("CCT2.REGISTER " + " CLIENTNAME " + TEST_APP_NAME_4 + " " + TEST_APP_NAME_4  )
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_4) == True

    res = client1.execute_command("CCT2.REGISTER " + " CLIENTNAME " + TEST_APP_NAME_5 + " " + TEST_APP_NAME_5  + " " + TEST_APP_GROUP_NAME_5)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_5) == True

    res = client1.execute_command("CCT2.REGISTER " +  TEST_APP_NAME_6  + " CLIENTNAME " + TEST_APP_NAME_6 + " " + TEST_APP_GROUP_NAME_6)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_6) == True

    res = client1.execute_command("CCT2.REGISTER " +  TEST_APP_NAME_7  + " CLIENTNAME " + TEST_APP_NAME_7 + " " + TEST_APP_GROUP_NAME_7 + " 8888")
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_7) == True


def test_heartbeat_with_client_name():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = 'test_search_with_client_name_app_1'
    TEST_APP_NAME_2 = 'test_search_with_client_name_app_2'
    TEST_APP_NAME_3 = 'test_search_with_client_name_app_3'

    TEST_APP_GROUP_NAME_3 = 'test_search_with_client_name_app_group_3'

    client1 = connect_redis_with_start()
    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_1) == True

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)

    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_2 + " CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_2) == True

    res = client1.execute_command("CCT2.HEARTBEAT CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)

    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_3 + " " + TEST_APP_GROUP_NAME_3 + " CLIENTNAME " + TEST_APP_NAME_3)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_3) == True

    res = client1.execute_command("CCT2.HEARTBEAT CLIENTNAME " + TEST_APP_NAME_3)
    assert cct_prepare.OK in str(res)

def test_heartbeat_with_client_name_2():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = 'test_search_with_client_name_app_1'
    TEST_APP_NAME_2 = 'test_search_with_client_name_app_2'

    client1 = connect_redis_with_start()
    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_1) == True

    res = client1.execute_command("CCT2.REGISTER " + TEST_APP_NAME_2 + " CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_2) == True

    time.sleep(1.6)

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)        

    res = client1.execute_command("CCT2.HEARTBEAT CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)

    time.sleep(1.6)

    res = client1.execute_command("CCT2.HEARTBEAT")
    assert cct_prepare.OK in str(res)    

    res = client1.execute_command("CCT2.HEARTBEAT CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)

    assert producer.exists(TEST_APP_NAME_1) == True
    assert producer.exists(TEST_APP_NAME_2) == True

    time.sleep(1.6)

    res = client1.execute_command("CCT2.HEARTBEAT CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)

    time.sleep(1.6)

    assert producer.exists(TEST_APP_NAME_1) == False
    assert producer.exists(TEST_APP_NAME_2) == True

    time.sleep(3.2)

    assert producer.exists(TEST_APP_NAME_2) == False


def test_search_with_client_name():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = '''test_search_with_client_name_app_1'''
    TEST_INDEX_NAME = '''test_search_with_client_name_index'''
    TEST_INDEX_PREFIX = '''test_search_with_client_name_index_prefix:'''

    TEST_APP_NAME_2 = '''test_search_with_client_name_app_2'''

    cct_prepare.create_index_with_prefix(producer, TEST_INDEX_PREFIX, TEST_INDEX_NAME)

     # ADD INITIAL DATA
    passport_value = "test_search_with_client_name_passport"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key1 = TEST_INDEX_PREFIX + str(1)
    producer.json().set(key1, Path.root_path(), d)

    # CLIENT 1
    client = connect_redis()
    res = client.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_1) == True

    # CLIENT 2
    res = client.execute_command("CCT2.REGISTER " + TEST_APP_NAME_2 + " CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_2) == True

    # QUERY FOR CLIENT 1
    res = client.execute_command("CCT2.FT.SEARCH " + TEST_INDEX_NAME + " @User\\.PASSPORT:{" + passport_value + "}")
    assert str(res) == '''[1, 'test_search_with_client_name_index_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_search_with_client_name_passport","Address":{"ID":"2000"}}}']]'''
    # QUERY FOR CLIENT 2
    res = client.execute_command("CCT2.FT.SEARCH " + TEST_INDEX_NAME + " @User\\.PASSPORT:{" + passport_value + "} CLIENTNAME " + TEST_APP_NAME_2)
    assert str(res) == '''[1, 'test_search_with_client_name_index_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_search_with_client_name_passport","Address":{"ID":"2000"}}}']]'''

    #UPDATE DATA
    d = cct_prepare.generate_single_object(9999 , 9999, passport_value)
    producer.json().set(key1, Path.root_path(), d)

    time.sleep(0.1)

    #CHECK STREAMS
    from_stream = client.xread(streams={TEST_APP_NAME_1:0} )
    #print(from_stream)
    assert '''test_search_with_client_name_index_prefix:1''' in str(from_stream)

    from_stream = client.xread(streams={TEST_APP_NAME_2:0} )
    #print(from_stream)
    assert '''test_search_with_client_name_index_prefix:1''' in str(from_stream)


def test_search_with_client_name_2():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_APP_NAME_1 = '''test_search_with_client_name_app_1'''
    TEST_INDEX_NAME = '''test_search_with_client_name_index'''
    TEST_INDEX_PREFIX = '''test_search_with_client_name_index_prefix:'''

    TEST_APP_NAME_2 = '''test_search_with_client_name_app_2'''

    cct_prepare.create_index_with_prefix(producer, TEST_INDEX_PREFIX, TEST_INDEX_NAME)

     # ADD INITIAL DATA
    passport_value = "test_search_with_client_name_passport"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key1 = TEST_INDEX_PREFIX + str(1)
    producer.json().set(key1, Path.root_path(), d)

    # CLIENT 1
    client = connect_redis()
    res = client.execute_command("CCT2.REGISTER " + TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_1) == True

    # CLIENT 2
    res = client.execute_command("CCT2.REGISTER " + TEST_APP_NAME_2 + " CLIENTNAME " + TEST_APP_NAME_2)
    assert cct_prepare.OK in str(res)
    assert producer.exists(TEST_APP_NAME_2) == True

    # QUERY FOR CLIENT 1
    non_passport_value = "non_passport_value"
    res = client.execute_command("CCT2.FT.SEARCH " + TEST_INDEX_NAME + " @User\\.PASSPORT:{" + non_passport_value + "}")
    assert str(res) == '''[0]'''
    # QUERY FOR CLIENT 2
    res = client.execute_command("CCT2.FT.SEARCH " + TEST_INDEX_NAME + " @User\\.PASSPORT:{" + passport_value + "} CLIENTNAME " + TEST_APP_NAME_2)
    assert str(res) == '''[1, 'test_search_with_client_name_index_prefix:1', ['$', '{"User":{"ID":"1000","PASSPORT":"test_search_with_client_name_passport","Address":{"ID":"2000"}}}']]'''

    #UPDATE DATA
    d = cct_prepare.generate_single_object(9999 , 9999, passport_value)
    producer.json().set(key1, Path.root_path(), d)

    time.sleep(0.1)

    #CHECK STREAMS
    from_stream = client.xread(streams={TEST_APP_NAME_1:0} )
    assert '''test_search_with_client_name_index_prefix:1''' not in str(from_stream)

    from_stream = client.xread(streams={TEST_APP_NAME_2:0} )
    assert '''test_search_with_client_name_index_prefix:1''' in str(from_stream)