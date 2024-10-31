import pytest
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC, \
                CCT_QUERY_HALF_TTL, CCT_QUERY_TTL, CCT_MODULE_PREFIX
from cct_test_utils import check_query_meta_data, get_redis_snapshot

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

@pytest.mark.skipif(True ,
                    reason="Feature is disabled")
def test_query_expired_offline():
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
    first_query_normalized = "User\\.PASSPORT:aaa"

    # KILL REDIS
    kill_redis()

    # PASS TIME
    time.sleep(CCT_QUERY_TTL)

    # START REDIS AGAIN
    producer = connect_redis_with_start()

    # PASS TIME
    time.sleep(1)

    # Check Meta data after query is expired offline
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False]*6 )

@pytest.mark.skipif(True ,
                    reason="Feature is disabled")
def test_query_expired_with_not_expired_offline():
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
    first_query_normalized = "User\\.PASSPORT:aaa"

    # PASS TIME
    time.sleep(CCT_QUERY_HALF_TTL)

    # KILL REDIS
    kill_redis()

    # START REDIS AGAIN
    producer = connect_redis_with_start()

    # PASS TIME
    time.sleep(1)

    # SECOND CLIENT
    query_value = passport_value
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") # match first two item


    # Check Meta data after query is expired offline
    get_redis_snapshot()
    check_query_meta_data(producer, cct_prepare.TEST_APP_NAME_1, first_query_normalized, key_1, [False, False, True, False, True, False] )