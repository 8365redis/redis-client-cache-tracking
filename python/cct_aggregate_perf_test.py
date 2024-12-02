import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import SKIP_PERF_TEST
import time


@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_ft_aggregate_with_many_keys():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    total = 100000
    # ADD INITIAL DATA
    for i in range(total):
        d = cct_prepare.generate_single_object(1000000 + i , 2000000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 )
    assert cct_prepare.OK in str(resp)

    # REQUEST
    client1.execute_command("cct2.ft.aggregate usersJsonIdx * GROUPBY 1 @User.ID")

    for i in range(120):
        res = client1.execute_command("CCT2.HEARTBEAT")
        assert str(res) == '''OK'''
        time.sleep(2)

    time.sleep(2)


@pytest.mark.skipif(False ,
                    reason="Only run manually")
def test_ft_aggregate_with_many_keys_and_groupby():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    total = 100000
    # ADD INITIAL DATA
    for i in range(total):
        if i % 2 == 0:
            d = cct_prepare.generate_single_object(1000000 + i , 2000000 - i, "aaa")
            r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
        else:
            d = cct_prepare.generate_single_object(1000000 + i , 2000000 - i, "bbb")
            r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # Connect
    client1 = connect_redis()

    # REQUEST
    start = time.time()
    res = client1.execute_command("ft.aggregate usersJsonIdx * GROUPBY 1 @User.PASSPORT")
    end = time.time()
    print(res)
    print("Time taken: ", end - start)