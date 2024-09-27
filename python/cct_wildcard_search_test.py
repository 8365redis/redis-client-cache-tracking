import redis
import pytest
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC, I2C, CCT_EOS
import time
from constants import SKIP_UNSTABLE_TEST

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

@pytest.mark.skipif(SKIP_UNSTABLE_TEST ,
                    reason="Only run manually")
def test_basic_wildcard_query_add_new_data():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    passport_value = "aaa"
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        r.json().set(key, Path.root_path(), d)

    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert "2000" in str(resp)

    key_exists = r.exists('CCT2:QC:usersJsonIdx:*:app1')
    assert key_exists == 1

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "usersJsonIdx:*" not in str(from_stream)

    # ADD A NEW DATA
    d = cct_prepare.generate_single_object(9999 , 9999, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(10000)
    r.json().set(key, Path.root_path(), d)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)


@pytest.mark.skipif(SKIP_UNSTABLE_TEST ,
                    reason="Only run manually")
def test_basic_wildcard_query_update_data():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    passport_value = "aaa"
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        r.json().set(key, Path.root_path(), d)

    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert "2000" in str(resp)

    key_exists = r.exists('CCT2:QC:usersJsonIdx:*:app1')
    assert key_exists == 1

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "usersJsonIdx:*" not in str(from_stream)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(9999 , 9999, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(50)
    r.json().set(key, Path.root_path(), d)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:50', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)


@pytest.mark.skipif(SKIP_UNSTABLE_TEST ,
                    reason="Only run manually")
def test_basic_wildcard_query_delete_data():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    passport_value = "aaa"
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        r.json().set(key, Path.root_path(), d)

    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert "2000" in str(resp)

    key_exists = r.exists('CCT2:QC:usersJsonIdx:*:app1')
    assert key_exists == 1

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "usersJsonIdx:*" not in str(from_stream)

    # DELETE DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(5)
    r.delete(key, Path.root_path())

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'DELETE', 'key': 'users:5', 'value': '', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)


@pytest.mark.skipif(SKIP_UNSTABLE_TEST ,
                    reason="Only run manually")
def test_wildcard_query_client_group_tracking():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    passport_value = "aaa"
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        r.json().set(key, Path.root_path(), d)

    # CLIENTS
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    client3 = connect_redis()
    client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3 + " " + cct_prepare.TEST_GROUP_NAME_1 )

    resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert "2000" in str(resp)

    key_exists = r.exists('CCT2:QC:usersJsonIdx:*:grp1')
    assert key_exists == 1

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "new_query" not in str(from_stream)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert "new_query" in str(from_stream)
    assert "usersJsonIdx:*" in str(from_stream)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert "new_query" in str(from_stream)
    assert "usersJsonIdx:*" in str(from_stream)

    # TRIM STREAMS
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    client2.xtrim(cct_prepare.TEST_APP_NAME_2 , 0)
    client3.xtrim(cct_prepare.TEST_APP_NAME_3 , 0)

    # ADD A NEW DATA
    d = cct_prepare.generate_single_object(9999 , 9999, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(10000)
    r.json().set(key, Path.root_path(), d)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert """{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}""" in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert """{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}""" in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert """{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}""" in str(from_stream)

    # TRIM STREAMS
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    client2.xtrim(cct_prepare.TEST_APP_NAME_2 , 0)
    client3.xtrim(cct_prepare.TEST_APP_NAME_3 , 0)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(5555 , 5555, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(50)
    r.json().set(key, Path.root_path(), d)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:50', 'value': '{"User":{"ID":"5555","PASSPORT":"aaa","Address":{"ID":"5555"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:50', 'value': '{"User":{"ID":"5555","PASSPORT":"aaa","Address":{"ID":"5555"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:50', 'value': '{"User":{"ID":"5555","PASSPORT":"aaa","Address":{"ID":"5555"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)

    # TRIM STREAMS
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    client2.xtrim(cct_prepare.TEST_APP_NAME_2 , 0)
    client3.xtrim(cct_prepare.TEST_APP_NAME_3 , 0)

    # DELETE DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(5)
    r.delete(key, Path.root_path())

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'DELETE', 'key': 'users:5', 'value': '', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert '''{'operation': 'DELETE', 'key': 'users:5', 'value': '', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert '''{'operation': 'DELETE', 'key': 'users:5', 'value': '', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)


def test_wildcard_query_in_snapshot():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    passport_value = "aaa"
    for i in range(20):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)

        r.json().set(key, Path.root_path(), d)

    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert "2000" in str(resp)

    key_exists = client1.exists('CCT2:QC:usersJsonIdx:*:app1')
    assert key_exists == 1

    # Check stream is empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    time.sleep(0.2)

    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert ''' {'operation': 'UPDATE', 'key': '', 'value': '', 'queries': '@usersJsonIdx:{*}'}''' in str(from_stream)


@pytest.mark.skipif(SKIP_UNSTABLE_TEST ,
                    reason="Only run manually")
def test_wildcard_query_in_snapshot_in_tracking_group():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    passport_value = "aaa"
    for i in range(3):
        d = cct_prepare.generate_single_object(1000 - i  , 2000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)

        r.json().set(key, Path.root_path(), d)

    # CLIENTS
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1 + " " + cct_prepare.TEST_GROUP_NAME_1 )
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_GROUP_NAME_1 )
    client3 = connect_redis()
    client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3 + " " + cct_prepare.TEST_GROUP_NAME_1 )


    resp = client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " *")
    assert "2000" in str(resp)

    key_exists = client1.exists('CCT2:QC:usersJsonIdx:*:grp1')
    assert key_exists == 1

    # Check stream is empty
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert CCT_EOS in from_stream[0][1][0][1]
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert '''{'operation': 'new_query', 'key': 'users:2', 'value': '{"User":{"ID":"998","PASSPORT":"aaa","Address":{"ID":"2002"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    assert '''{'operation': 'new_query', 'key': 'users:1', 'value': '{"User":{"ID":"999","PASSPORT":"aaa","Address":{"ID":"2001"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    assert '''{'operation': 'new_query', 'key': 'users:0', 'value': '{"User":{"ID":"1000","PASSPORT":"aaa","Address":{"ID":"2000"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert '''{'operation': 'new_query', 'key': 'users:2', 'value': '{"User":{"ID":"998","PASSPORT":"aaa","Address":{"ID":"2002"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    assert '''{'operation': 'new_query', 'key': 'users:1', 'value': '{"User":{"ID":"999","PASSPORT":"aaa","Address":{"ID":"2001"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    assert '''{'operation': 'new_query', 'key': 'users:0', 'value': '{"User":{"ID":"1000","PASSPORT":"aaa","Address":{"ID":"2000"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)


    # DISCONNECT
    client2.connection_pool.disconnect()
    client3.connection_pool.disconnect()

    time.sleep(0.2)

    # RE-REGISTER
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2 + " " + cct_prepare.TEST_GROUP_NAME_1)
    client3 = connect_redis()
    client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3 + " " + cct_prepare.TEST_GROUP_NAME_1)

    time.sleep(0.2)

    # CHECK STREAMS
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert '''{'operation': 'UPDATE', 'key': '', 'value': '', 'queries': '@usersJsonIdx:{*}''' in str(from_stream)
    from_stream = client3.xread( streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert '''{'operation': 'UPDATE', 'key': '', 'value': '', 'queries': '@usersJsonIdx:{*}''' in str(from_stream)

    # TRIM STREAMS
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    client2.xtrim(cct_prepare.TEST_APP_NAME_2 , 0)
    client3.xtrim(cct_prepare.TEST_APP_NAME_3 , 0)

    # ADD A NEW DATA
    d = cct_prepare.generate_single_object(9999 , 9999, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(10000)
    r.json().set(key, Path.root_path(), d)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert """{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}""" in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert """{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}""" in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert """{'operation': 'UPDATE', 'key': 'users:10000', 'value': '{"User":{"ID":"9999","PASSPORT":"aaa","Address":{"ID":"9999"}}}', 'queries': 'usersJsonIdx:*'}""" in str(from_stream)

    # TRIM STREAMS
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    client2.xtrim(cct_prepare.TEST_APP_NAME_2 , 0)
    client3.xtrim(cct_prepare.TEST_APP_NAME_3 , 0)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(5555 , 5555, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    r.json().set(key, Path.root_path(), d)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:1', 'value': '{"User":{"ID":"5555","PASSPORT":"aaa","Address":{"ID":"5555"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:1', 'value': '{"User":{"ID":"5555","PASSPORT":"aaa","Address":{"ID":"5555"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert '''{'operation': 'UPDATE', 'key': 'users:1', 'value': '{"User":{"ID":"5555","PASSPORT":"aaa","Address":{"ID":"5555"}}}', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)

    # TRIM STREAMS
    client1.xtrim(cct_prepare.TEST_APP_NAME_1 , 0)
    client2.xtrim(cct_prepare.TEST_APP_NAME_2 , 0)
    client3.xtrim(cct_prepare.TEST_APP_NAME_3 , 0)

    # DELETE DATA
    key = cct_prepare.TEST_INDEX_PREFIX + str(0)
    r.delete(key, Path.root_path())

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'operation': 'DELETE', 'key': 'users:0', 'value': '', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert '''{'operation': 'DELETE', 'key': 'users:0', 'value': '', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)
    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_3:0} )
    assert '''{'operation': 'DELETE', 'key': 'users:0', 'value': '', 'queries': 'usersJsonIdx:*'}''' in str(from_stream)


def test_basic_wildcard_query_add_new_data_with_multi_index():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first

    TEST_INDEX_NAME_1 = "index_1"
    TEST_INDEX_PREFIX_1 = "index_1_prefix:"
    schema = (TagField("$.a", as_name="a"), TagField("$.b", as_name="b"),  \
              TagField("$.c", as_name="c"), TagField("$.d", as_name="d"))
    r.ft(TEST_INDEX_NAME_1).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX_1], index_type=IndexType.JSON))

    TEST_INDEX_NAME_2 = "index_2"
    TEST_INDEX_PREFIX_2 = "index_2_prefix:"
    schema = (TagField("$.x", as_name="x"), TagField("$.y", as_name="y"),  \
              TagField("$.z", as_name="z"), TagField("$.q", as_name="q"))
    r.ft(TEST_INDEX_NAME_2).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX_2], index_type=IndexType.JSON))

    #ADD DATA INDEX_1
    for i in range(3):
        d = { "a" : str(i) , "b" : str(i+1000) , "c" : str(i+2000), "d" : str(i+3000)}
        key = TEST_INDEX_PREFIX_1 + str(i)
        r.json().set(key, Path.root_path(), d)

    #ADD DATA INDEX_2
    for i in range(3):
        d = { "x" : str(i) , "y" : str(i+6000) , "z" : str(i+7000), "q" : str(i+8000)}
        key = TEST_INDEX_PREFIX_2 + str(i)
        r.json().set(key, Path.root_path(), d)

    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    resp = r.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME_1 + " *")
    assert '''[3, 'index_1_prefix:0', ['$', '{"a":"0","b":"1000","c":"2000","d":"3000"}'], 'index_1_prefix:1', ['$', '{"a":"1","b":"1001","c":"2001","d":"3001"}'], 'index_1_prefix:2', ['$', '{"a":"2","b":"1002","c":"2002","d":"3002"}']]''' == str(resp)

    key_exists = r.exists('CCT2:QC:' + TEST_INDEX_NAME_1 + ':*:app1')
    assert key_exists == 1

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert "usersJsonIdx:*" not in str(from_stream)

    # ADD A NEW DATA
    d = { "a" : str(i) , "b" : str(i+1000) , "c" : str(i+2000), "d" : str(i+3000)}
    key = TEST_INDEX_PREFIX_2 + str(1)
    r.json().set(key, Path.root_path(), d)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    assert '''{'-END_OF_SNAPSHOT-': '-END_OF_SNAPSHOT-'}''' in str(from_stream)

    from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_2:0} )
    assert from_stream == []


