import redis
import pytest
import time
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")


def test_basic_tracking_update_to_stream():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + passport_value + "}")

    # UPDATE DATA
    #d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    #producer.json().set(key, Path.root_path(), d)

    # Check update is not written to client 1 stream
    from_stream = client1.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(from_stream)
    assert 2 == len(from_stream[0])

    # Check update is written to client 2 stream
    from_stream = client1.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    #print(from_stream)
    assert key == str(from_stream[0][1][1][1]['key'])
    assert "new_query" == str(from_stream[0][1][1][1]['operation'])

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_GROUP_NAME_1)
    assert tracked_key 