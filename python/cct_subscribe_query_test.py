import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from cct_test_utils import get_redis_snapshot 
from manage_redis import connect_redis, connect_redis_with_start, kill_redis


@pytest.fixture(autouse=True)
def before_and_after_test():
    yield
    kill_redis()

def test_basic_subscribe_query_test_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(1))

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    resp = client1.execute_command("CCT2.SUBSCRIBE_TO_QUERY "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")
    print(resp)
