import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_MODULE_TRACKING_PREFIX

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")


def test_new_key_added_no_affect():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # NEW DATA
    d = cct_prepare.generate_single_object(1001 , 2002, "bbb")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    # Check stream is empty
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert not from_stream

    # Check new key is not tracked    
    tracked_key = producer.sismember(CCT_MODULE_TRACKING_PREFIX + cct_prepare.TEST_INDEX_PREFIX + str(2), cct_prepare.TEST_APP_NAME_1)
    assert not tracked_key 
