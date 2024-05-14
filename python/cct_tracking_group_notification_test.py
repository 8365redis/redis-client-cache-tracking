import redis
import pytest
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

def test_basic_tracking_data():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(1))

    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_GROUP_NAME_1 )
    assert cct_prepare.OK in str(resp)
    print(resp)

    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_GROUP_NAME_1 )
    assert cct_prepare.OK in str(resp)
    print(resp)

    query_key_attr = "User\\.PASSPORT" + ":" + d["User"]["PASSPORT"]
    print("query_key_attr:" + query_key_attr)
    # SEARCH
    resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + d["User"]["PASSPORT"] + "}")
    assert resp
    print("CCT.FT.SEARCH Resp:" + str(resp))

    #CHECK TRACKED QUERY
    tracked_query = r.sismember(CCT_Q2C + query_key_attr, cct_prepare.TEST_GROUP_NAME_1)
    assert tracked_query

    #CHECK TRACKED KEY
    tracked_key = r.sismember(CCT_K2C + cct_prepare.TEST_INDEX_PREFIX + str(1), cct_prepare.TEST_GROUP_NAME_1)
    assert tracked_key 


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
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    client2 = connect_redis()
    client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    query_value = "bbb"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in stream for client 1
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert key in str(from_stream[0][1])

    # Check key is in stream for client 2
    from_stream = client2.xread( count=2, streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert key in str(from_stream[0][1])

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_GROUP_NAME_1)
    assert tracked_key 

def test_basic_tracking_update_to_stream_multi_client():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    producer.json().set(key, Path.root_path(), d)

    clients = []
    # FIRST CLIENT
    for i in range(20):
        client = connect_redis()
        clients.append(client)
        client.execute_command("CCT.REGISTER app" + str(i) + " " + cct_prepare.TEST_GROUP_NAME_1 )

    query_value = "bbb"
    client.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    producer.json().set(key, Path.root_path(), d)

    i = 0
    for c in clients:
        stream_name = "app" + str(i)
        from_stream = c.xread( count=2, streams={stream_name:0} )
        assert key in str(from_stream[0][1])
        i = i + 1

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_GROUP_NAME_1)
    assert tracked_key 


def test_not_tracking_not_effected():
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
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    client2 = connect_redis()
    client2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_GROUP_NAME_2 )

    query_value = "bbb"
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, query_value)
    producer.json().set(key, Path.root_path(), d)

    # Check key is in stream for client 1
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert key in str(from_stream[0][1])

    # Check key is not stream for client 2
    from_stream = client1.xread( count=2, streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert key not in str(from_stream[0][1])

    # Check new key is tracked    
    tracked_key = producer.sismember(CCT_K2C + key, cct_prepare.TEST_GROUP_NAME_1)
    assert tracked_key 