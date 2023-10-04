import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis_with_start, kill_redis

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_unique_id_tracking_test_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)
    data = cct_prepare.generate_input(10)
    cct_prepare.add_list(r, data)

    
    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    print(resp)
    
    # SEARCH
    print(data[0])
    resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "}")
    print(resp)

    # CHANGE first item
    id = resp[0]
    new_d = {}
    cct_prepare.generate_object(new_d,12,22)
    resp = r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(id), Path.root_path(), new_d)
    print(resp)

    # CHECK STREAM
    resp = r.xread( count=1, streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(resp)   


def test_multi_register_handle_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first

    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # REGISTER 2. Attempt 
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # REGISTER 3. Attempt
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # REGISTER with different app name from same client
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_2)


def test_ft_search_result_comparison_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)
    data = cct_prepare.generate_input(10)
    cct_prepare.add_list(r, data)

    # REGISTER
    resp = r.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)
    
    # SEARCH WITH CCT
    cct_resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "}")
    # SEARCH WITH DEFAULT
    default_resp = r.execute_command("FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "}")
    assert cct_resp == default_resp

    # SEARCH WITH CCT MULTI
    cct_resp = r.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
    print(cct_resp)
    # SEARCH WITH DEFAULT MULTI
    default_resp = r.execute_command("FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
    print(default_resp)
    assert cct_resp == default_resp    

 




