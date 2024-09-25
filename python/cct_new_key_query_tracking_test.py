import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_K2C, CCT_EOS

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
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # NEW DATA
    d = cct_prepare.generate_single_object(1001 , 2002, "bbb")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    # Check stream is empty
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # Check new key is not tracked    
    tracked_key = producer.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(2), cct_prepare.TEST_APP_NAME_1)
    assert not tracked_key 

def test_new_key_added__affect_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)    
    
    passport_value = "aaa"

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # NEW DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)

    # Check stream is empty
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert (cct_prepare.TEST_INDEX_PREFIX + str(1)) in str(from_stream[0][1])

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(1), cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 