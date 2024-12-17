import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from cct_test_utils import get_redis_snapshot 
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
import time

CCT_SUBSCRIBED_INDEX = "CCT2:SIDX"

@pytest.fixture(autouse=True)
def before_and_after_test():
    yield
    kill_redis()

def test_basic_subscribe_index_test_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    passport_value = "aaa"
    for i in range(0, 5):
        d = cct_prepare.generate_single_object(1000 - i , 2000 + i, passport_value)
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(i))


    resp = r.execute_command("CCT2.SETUP_INDEX_SUBSCRIPTION "+ cct_prepare.TEST_INDEX_NAME)
    print(resp)

    time.sleep(0.1)

    resp = r.execute_command("SMEMBERS " + CCT_SUBSCRIBED_INDEX)
    print(resp)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    resp = r.execute_command("SISMEMBER " + CCT_SUBSCRIBED_INDEX + " " + cct_prepare.TEST_INDEX_NAME)
    print(resp)
    assert resp == 1

    # SUBSCRIBE
    stream_name = client1.execute_command("CCT2.SUBSCRIBE_TO_INDEX " + cct_prepare.TEST_INDEX_NAME)
    print(stream_name)
    assert cct_prepare.TEST_INDEX_NAME in str(stream_name)

    # READ all stream data
    from_stream1 = client1.xread( streams={stream_name:0} )
    print(from_stream1)

    client1.xtrim(stream_name , 0)

    time.sleep(0.1)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1005 , 2005, "fff")
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(5), Path.root_path(), d)

    # READ all stream data
    from_stream1 = client1.xread( streams={stream_name:0} )
    print(from_stream1)

    client1.xtrim(stream_name , 0)

    r.delete(cct_prepare.TEST_INDEX_PREFIX + str(1))

    time.sleep(0.1)

    # READ all stream data
    from_stream1 = client1.xread( streams={stream_name:0} )
    print(from_stream1)



