import redis
import pytest
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC
import time


@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

def test_index_created_after_keys_added():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    
    time.sleep(0.1)

    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    time.sleep(0.1)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)

    # make search in not existing index
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    except redis.exceptions.ResponseError as e:
        assert "Success" in str(e)

    # INDEX CREATED HERE
    cct_prepare.create_index(r)

    time.sleep(1.1)

    resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert '''{"User":{"ID":"1002","PASSPORT":"aaa","Address":{"ID":"1998"}}}''' in str(resp)


def test_index_created_after_keys_added_2():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    
    time.sleep(0.1)

    TEST_INDEX_NAME_1 = "index_1"
    TEST_INDEX_PREFIX_1 = "index_1_prefix:"
    TEST_INDEX_NAME_2 = "index_2"
    TEST_INDEX_PREFIX_2 = "index_2_prefix:"

    #ADD DATA INDEX_1
    for i in range(3):
        d = { "a" : str(i) , "b" : str(i+1000) , "c" : str(i+2000), "d" : str(i+3000)}
        key = TEST_INDEX_PREFIX_1 + str(i)
        r.json().set(key, Path.root_path(), d)

    time.sleep(0.1)

    # REGISTER 1
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)

    # make search in not existing index
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME_1 + " *")
    except redis.exceptions.ResponseError as e:
        assert "Success" in str(e)

    # CREATE INDEX 1
    schema = (TagField("$.a", as_name="a"), TagField("$.b", as_name="b"),  \
              TagField("$.c", as_name="c"), TagField("$.d", as_name="d"))
    r.ft(TEST_INDEX_NAME_1).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX_1], index_type=IndexType.JSON))

    time.sleep(1.1)
   
    resp = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME_1 + " *")
    assert '''{"a":"2","b":"1002","c":"2002","d":"3002"}''' in str(resp) 

    # make search in not existing index
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME_2 + " *")
    except redis.exceptions.ResponseError as e:
        assert "Success" in str(e)

    # CREATE INDEX 2
    TEST_INDEX_NAME_2 = "index_2"
    TEST_INDEX_PREFIX_2 = "index_2_prefix:"
    schema = (TagField("$.x", as_name="x"), TagField("$.y", as_name="y"),  \
              TagField("$.z", as_name="z"), TagField("$.q", as_name="q"))
    r.ft(TEST_INDEX_NAME_2).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX_2], index_type=IndexType.JSON))

    time.sleep(1.1)

    #ADD DATA INDEX_2
    for i in range(3):
        d = { "x" : str(i) , "y" : str(i+6000) , "z" : str(i+7000), "q" : str(i+8000)}
        key = TEST_INDEX_PREFIX_2 + str(i)
        r.json().set(key, Path.root_path(), d)

    resp = client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME_2 + " *")
    assert '''{"x":"0","y":"6000","z":"7000","q":"8000"}''' in str(resp)

def test_index_created_after_many_keys_added():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    
    time.sleep(0.1)

    # ADD INITIAL DATA
    for i in range(10000):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    time.sleep(0.1)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)

    # make search in not existing index
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    except redis.exceptions.ResponseError as e:
        assert "Success" in str(e)

    # INDEX CREATED HERE
    cct_prepare.create_index(r)

    time.sleep(1.1)

    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " * ")

    cct_prepare.flush_db(r) # clean all db lastly

def test_new_index_working_as_expected():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    
    time.sleep(0.1)

    # ADD INITIAL DATA
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    time.sleep(0.1)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_APP_NAME_1 +  " 2")
    assert cct_prepare.OK in str(resp)

    # make search in not existing index
    try:
        resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    except redis.exceptions.ResponseError as e:
        assert "Success" in str(e)

    key_exists = client1.exists('CCT2:QC:usersJsonIdx:*:app1')
    assert key_exists == 0

    # INDEX CREATED HERE
    cct_prepare.create_index(r)

    time.sleep(1.1)

    resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert "2000" in str(resp)

    key_exists = client1.exists('CCT2:QC:usersJsonIdx:*:app1')
    assert key_exists == 1

    from_stream = client1.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "usersJsonIdx:*" not in str(from_stream)

    # ADD A NEW DATA
    d = cct_prepare.generate_single_object(9999 , 9999, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(10000)    
    r.json().set(key, Path.root_path(), d)

    from_stream = client1.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}''' == str(from_stream[0][1][1][1])
