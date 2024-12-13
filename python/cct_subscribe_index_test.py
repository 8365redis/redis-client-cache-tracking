import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from cct_test_utils import get_redis_snapshot 
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
import time

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

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    resp = client1.execute_command("CCT2.SUBSCRIBE_TO_INDEX "+ cct_prepare.TEST_INDEX_NAME)
    print(resp)

    time.sleep(1)

    # REGISTER
    client2 = connect_redis()
    resp = client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)


