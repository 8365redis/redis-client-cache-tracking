import pytest
import time
import redis
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, SKIP_HB_TEST,  \
                CCT_NOT_REGISTERED_COMMAND_ERROR, CCT_HEART_BEAT_INTERVAL

import constants

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

@pytest.mark.skipif(SKIP_HB_TEST ,
                    reason="Not sending HB in other tests")
def test_client_expire_normal():
    producer = connect_redis()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    client1 = connect_redis()
    res = client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(res)
    res = client1.execute_command("CCT.HEARTBEAT")
    assert cct_prepare.OK in str(res)
    time.sleep(CCT_HEART_BEAT_INTERVAL * 3 + 1)