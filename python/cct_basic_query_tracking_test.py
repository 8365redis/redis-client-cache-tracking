import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_MODULE_CLIENT_PREFIX, CCT_MODULE_QUERY_PREFIX, CCT_MODULE_TRACKING_PREFIX

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_basic_query_tracking_test_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(1))

    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    print(resp)

    # CHECK REGISTER 
    client_id = r.client_id()
    app_name = r.get(CCT_MODULE_CLIENT_PREFIX + str(client_id))
    assert app_name == cct_prepare.TEST_APP_NAME_1

    query_key_attr = "User\\.PASSPORT" + ":" + d["User"]["PASSPORT"]
    print("query_key_attr:" + query_key_attr)
    # SEARCH
    resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + d["User"]["PASSPORT"] + "}")
    assert resp
    print("CCT.FT.SEARCH Resp:" + str(resp))

    #CHECK TRACKED QUERY
    tracked_query = r.sismember(CCT_MODULE_QUERY_PREFIX + query_key_attr, cct_prepare.TEST_APP_NAME_1)
    assert tracked_query

    #CHECK TRACKED KEY
    tracked_key = r.sismember(CCT_MODULE_TRACKING_PREFIX + cct_prepare.TEST_INDEX_PREFIX + str(1), cct_prepare.TEST_APP_NAME_1)
    assert tracked_key 

    # SAME CLIENT ADDS A NEW DATA THAT MATCHES TO QUERY
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    new_key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    r.json().set(new_key, Path.root_path(), d)
    assert r.json().get(cct_prepare.TEST_INDEX_PREFIX + str(2))

    # CHECK THE STREAM
    from_stream = r.xread( count=2, streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert new_key in str(from_stream[0][1])
    print("From Stream :" + str(from_stream[0][1]))
    


def test_basic_query_tracking_test_2():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(1), Path.root_path(), d)

    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    print(resp)

    # CHECK REGISTER 
    client_id = r.client_id()
    print("Client ID : " + str(client_id))
    app_name = r.get(CCT_MODULE_CLIENT_PREFIX + str(client_id))
    assert app_name == cct_prepare.TEST_APP_NAME_1

    # SEARCH
    resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + d["User"]["PASSPORT"] + "}")
    assert resp
    print(resp)

    ####### SECOND CLIENT

    # REGISTER
    r2 = connect_redis()
    resp = r2.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)
    print(resp)

    # CHECK REGISTER 
    client_id = r2.client_id()
    print("Client ID : " + str(client_id))
    app_name = r2.get(CCT_MODULE_CLIENT_PREFIX + str(client_id))
    assert app_name == cct_prepare.TEST_APP_NAME_2

    # SEARCH
    resp = r2.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + d["User"]["PASSPORT"] + "}")
    assert resp
    print(resp)

    ####### THIRD CLIENT

    # REGISTER
    r3 = connect_redis()
    resp = r3.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_3)
    assert cct_prepare.OK in str(resp)
    print(resp)

    # CHECK REGISTER 
    client_id = r3.client_id()
    print("Client ID : " + str(client_id))
    app_name = r3.get(CCT_MODULE_CLIENT_PREFIX + str(client_id))
    assert app_name == cct_prepare.TEST_APP_NAME_3

    # CLIENT ADDS A NEW DATA THAT MATCHES TO QUERY
    #d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    #r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(2), Path.root_path(), d)