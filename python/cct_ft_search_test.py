import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_Q2K

from cct_test_utils import get_redis_snapshot

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_more_than_10_result():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for x in range(30):
        d = cct_prepare.generate_single_object(1000 , 2000+x, passport_value)
        producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(str(x+1)), Path.root_path(), d)
    first_query_normalized = "User\\.PASSPORT:aaa"
    second_query_normalized = "User\\.ID:1000"

    # FIRST CLIENT DEFAULT
    query_value = passport_value
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}") 

    # GET ALL KEYS
    keys = producer.smembers(CCT_Q2K + first_query_normalized)

    # CHECK KEY COUNT
    default_count = 10
    assert(len(keys) == default_count)

    # FIRST CLIENT 20
    query_value = 1000
    limit = 20
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(query_value) + "}" + " LIMIT 0 " + str(limit))

    # GET ALL KEYS
    keys = producer.smembers(CCT_Q2K + second_query_normalized)

    # CHECK KEY COUNT
    assert(len(keys) == limit)


