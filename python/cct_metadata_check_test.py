import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_K2C, CCT_EOS
from cct_test_utils import get_redis_snapshot
import time

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")


def test_new_keys_added_after_matching_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    passport_value = "aaa"

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # Key existence
    assert producer.exists('CCT2:QC:usersJsonIdx:User\\.PASSPORT:aaa:app1') == 1
    # Set membership assertions
    assert producer.sismember('CCT2:K2C:users:0', 'app1') == 1
    assert producer.sismember('CCT2:K2Q:users:2', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:C2Q:app1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:K2Q:users:1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:K2C:users:1', 'app1') == 1
    assert producer.sismember('CCT2:Q2C:usersJsonIdx:User\\.PASSPORT:aaa', 'app1') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:2') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:0') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:1') == 1
    assert producer.sismember('CCT2:K2C:users:2', 'app1') == 1
    assert producer.sismember('CCT2:K2Q:users:0', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1


def test_keys_updated_after_matching_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    passport_value = "aaa"
    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # Key existence
    assert producer.exists('CCT2:QC:usersJsonIdx:User\\.PASSPORT:aaa:app1') == 1
    # Set membership assertions
    assert producer.sismember('CCT2:K2Q:users:1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:K2C:users:2', 'app1') == 1
    assert producer.sismember('CCT2:K2Q:users:0', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:0') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:2') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:1') == 1
    assert producer.exists('CCT2:QC:usersJsonIdx:User\\.PASSPORT:aaa:app1') == 1
    assert producer.sismember('CCT2:Q2C:usersJsonIdx:User\\.PASSPORT:aaa', 'app1') == 1
    assert producer.sismember('CCT2:K2C:users:1', 'app1') == 1
    assert producer.sismember('CCT2:K2C:users:0', 'app1') == 1
    assert producer.sismember('CCT2:C2Q:app1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    
    # UPDATE ALL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(100 - i  , 200 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)
    
    # Key existence
    assert producer.exists('CCT2:QC:usersJsonIdx:User\\.PASSPORT:aaa:app1') == 1

    # Set membership assertions
    assert producer.sismember('CCT2:K2Q:users:1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:K2C:users:2', 'app1') == 1
    assert producer.sismember('CCT2:K2Q:users:0', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:K2Q:users:2', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:0') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:2') == 1
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:1') == 1
    assert producer.exists('CCT2:QC:usersJsonIdx:User\\.PASSPORT:aaa:app1') == 1
    assert producer.sismember('CCT2:Q2C:usersJsonIdx:User\\.PASSPORT:aaa', 'app1') == 1
    assert producer.sismember('CCT2:K2C:users:1', 'app1') == 1
    assert producer.sismember('CCT2:K2C:users:0', 'app1') == 1
    assert producer.sismember('CCT2:C2Q:app1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1

def test_keys_deleted_after_matching_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    passport_value = "aaa"
    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    #get_redis_snapshot()

    # UPDATE ALL DATA
    for i in range(3):
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        producer.delete(key)

    time.sleep(0.1)

    #get_redis_snapshot()

    # Key existence
    assert producer.exists('CCT2:QC:usersJsonIdx:User\\.PASSPORT:aaa:app1') == 1

    # Set membership assertions
    assert producer.sismember('CCT2:K2Q:users:0', 'usersJsonIdx:User\\.PASSPORT:aaa') == 0
    assert producer.sismember('CCT2:K2Q:users:1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 0
    assert producer.sismember('CCT2:K2Q:users:2', 'usersJsonIdx:User\\.PASSPORT:aaa') == 0
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:1') == 0
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:0') == 0
    assert producer.sismember('CCT2:Q2K:usersJsonIdx:User\\.PASSPORT:aaa', 'users:2') == 0
    assert producer.sismember('CCT2:Q2C:usersJsonIdx:User\\.PASSPORT:aaa', 'app1') == 1
    assert producer.sismember('CCT2:C2Q:app1', 'usersJsonIdx:User\\.PASSPORT:aaa') == 1
    assert producer.sismember('CCT2:K2C:users:0', 'app1') == 0
    assert producer.sismember('CCT2:K2C:users:1', 'app1') == 0
    assert producer.sismember('CCT2:K2C:users:2', 'app1') == 0