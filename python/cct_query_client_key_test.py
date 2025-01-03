import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_QC, CCT_DELI

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

def test_query_client_key_match_single_client():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key_1 = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key_1, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    query_1 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa" + CCT_DELI

    # CHECK QUERY:CLIENT = 1
    result = producer.exists(CCT_QC + query_1 + cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.exists(CCT_QC + query_1 + "NOT_EXISTING_CLIENT")
    assert not result

def test_query_client_key_match_multi_client():
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
    d = cct_prepare.generate_single_object(1002 , 2002, passport_value)
    key_3 = cct_prepare.TEST_INDEX_PREFIX + str(3)
    producer.json().set(key_3, Path.root_path(), d)   

    # FIRST CLIENT All keys
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}") 

    # SECOND CLIENT Second key only
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(1001) + "}")

    # THIRD CLIENT Third key only
    client3 = connect_redis()
    client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3)
    client3.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(1002) + "}")

    # FOURTH CLIENT 2&3 
    client4 = connect_redis()
    client4.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_4)
    client4.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(1001) + "}")
    client4.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(1002) + "}")

    query_1 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa" + CCT_DELI
    query_2 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.ID:1001" + CCT_DELI
    query_3 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.ID:1002" + CCT_DELI
    
    # CHECK CLIENT1
    result = producer.exists(CCT_QC + query_1 + cct_prepare.TEST_APP_NAME_1)
    assert result
    result = producer.exists(CCT_QC + query_1 + "non_existing_key")
    assert not result

    # CHECK CLIENT2
    result = producer.exists(CCT_QC + query_2 + cct_prepare.TEST_APP_NAME_2)
    assert result

    # CHECK CLIENT3
    result = producer.exists(CCT_QC + query_3 + cct_prepare.TEST_APP_NAME_3)
    assert result       


    # CHECK CLIENT4
    result = producer.exists(CCT_QC + query_2 + cct_prepare.TEST_APP_NAME_4)
    assert result
    result = producer.exists(CCT_QC + query_3 + cct_prepare.TEST_APP_NAME_4)
    assert result


def test_query_client_key_match_multi_client_multi_key():
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
    d = cct_prepare.generate_single_object(1001 , 2002, passport_value)
    key_3 = cct_prepare.TEST_INDEX_PREFIX + str(3)
    producer.json().set(key_3, Path.root_path(), d)   

    # FIRST CLIENT All keys
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}") 

    # SECOND CLIENT 2&3
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(1001) + "}")

    query_1 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.PASSPORT:aaa" + CCT_DELI
    query_2 = cct_prepare.TEST_INDEX_NAME + CCT_DELI + "User\\.ID:1001" + CCT_DELI

    # CHECK CLIENT1
    result = producer.exists(CCT_QC + query_1 + cct_prepare.TEST_APP_NAME_1)
    assert result

    # CHECK CLIENT2
    result = producer.exists(CCT_QC + query_2 + cct_prepare.TEST_APP_NAME_2)
    assert result
