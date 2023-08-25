from manage_redis import connect_redis, connect_redis_with_start, kill_redis
import cct_prepare
import pytest


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_start_chaos():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    data = cct_prepare.generate_input_for_chaos(100,100,100,100)
    cct_prepare.add_list(r, data)

    