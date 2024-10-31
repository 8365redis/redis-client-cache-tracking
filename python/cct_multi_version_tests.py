import pytest
import redis
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis_with_start, connect_redis
import cct_prepare
from constants import CCT_K2C, CCT_Q2C, SKIP_MULTI_VERSION_TEST

@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")


#to run this test you need to load cctv1 module and run the redis manually
@pytest.mark.skipif(SKIP_MULTI_VERSION_TEST ,
                    reason="Only run manually")
def test_register_diff_version():
    producer = connect_redis()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # REGISTER CLIENT1
    client1 = connect_redis()
    resp = client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # REGISTER CLIENT2
    client2 = connect_redis()
    resp = client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)

    # REQUEST CLIENT1 in V2
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "asasa" + "}")
    except redis.exceptions.ResponseError as e:
        assert "Not registered client" in str(e)

    #DISCONNECT CLIENT1
    client1.connection_pool.disconnect()

    # REQUEST CLIENT2
    resp = client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "asasa" + "}")
    assert resp[0] == 0
    
    # REQUEST CLIENT1 in V2
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "asasa" + "}")
    except redis.exceptions.ResponseError as e:
        assert "Not registered client" in str(e)

    # REQUEST CLIENT1 in V1
    try:
        resp = client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "asasa" + "}")
    except redis.exceptions.ResponseError as e:
        assert "Not registered client" in str(e)


#to run this test you need to load cctv1 module and run the redis manually
@pytest.mark.skipif(SKIP_MULTI_VERSION_TEST ,
                    reason="Only run manually")
def test_register_diff_search_not_mixed():
    producer = connect_redis()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD DATA
    d = cct_prepare.generate_single_object(1000 , 2000 , "aaa")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(0), Path.root_path(), d)
    d = cct_prepare.generate_single_object(1001 , 2001 , "bbb")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    d = cct_prepare.generate_single_object(1002 , 2002 , "ccc")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    # REGISTER CLIENT1
    client1 = connect_redis()
    resp = client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # REGISTER CLIENT2
    client2 = connect_redis()
    resp = client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)

    # REQUEST CLIENT1 in V1
    resp = client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "bbb" + "}")
    assert str(resp) == '''[1, 'users:1', ['$', '{"User":{"ID":"1001","PASSPORT":"bbb","Address":{"ID":"2001"}}}']]'''

    # REQUEST CLIENT2 in V2
    resp = client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "aaa" + "}")
    assert str(resp) == '''[1, 'users:0', ['$', '{"User":{"ID":"1000","PASSPORT":"aaa","Address":{"ID":"2000"}}}']]'''

    # REQUEST CLIENT1 in V2
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "asasa" + "}")
    except redis.exceptions.ResponseError as e:
        assert "Not registered client" in str(e)

    # REQUEST CLIENT2 in V1
    try:
        resp = client2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "zzzz" + "}")
    except redis.exceptions.ResponseError as e:
        assert "Not registered client" in str(e)

    # MAKE SAME REQUEST
    resp = client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "ccc" + "}")
    assert str(resp) == '''[1, 'users:2', ['$', '{"User":{"ID":"1002","PASSPORT":"ccc","Address":{"ID":"2002"}}}']]'''
    resp = client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + "ccc" + "}")
    assert str(resp) == '''[1, 'users:2', ['$', '{"User":{"ID":"1002","PASSPORT":"ccc","Address":{"ID":"2002"}}}']]'''

    # UPDATE DATA (K2)
    d = cct_prepare.generate_single_object(1002 , 2002, "ddd")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)

    # CHECK STREAMS
    from_stream1 = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    from_stream1 = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert from_stream1 == from_stream1

    # TRIM STREAMS
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    client2.xtrim(cct_prepare.TEST_APP_NAME_2 , 0)

    # UPDATE DATA (K0)
    d = cct_prepare.generate_single_object(1000 , 2000, "zzz")
    producer.json().set(cct_prepare.TEST_INDEX_PREFIX + str(0), Path.root_path(), d)

    # CHECK STREAMS
    from_stream1 = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert from_stream1 == []
    from_stream2 = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert from_stream2[0][1][0][1]['key'] == 'users:0'