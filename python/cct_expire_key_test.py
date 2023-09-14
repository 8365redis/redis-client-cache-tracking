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

def test_basic_key_expired():
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