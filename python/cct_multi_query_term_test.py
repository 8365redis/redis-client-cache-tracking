import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis, start_redis
import cct_prepare
from constants import CCT_K2C, CCT_Q_DELI, CCT_EOS
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_all_query_match():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{aaa}")

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{aaa} @User\\.ID:{1000}')

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{aaa} @User\\.ID:{1000} @User\\.Address\\.ID:{2000}')

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)


def test_one_query_match_multi_doesnt():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1001 , 2001, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{aaa}")

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{aaa} @User\\.ID:{1000}')

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{aaa} @User\\.ID:{1000} @User\\.Address\\.ID:{2000}')

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2002, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_two_query_match_multi_doesnt():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1000 , 2001, "aaa")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{aaa}")

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{aaa} @User\\.ID:{1000}')

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{aaa} @User\\.ID:{1000} @User\\.Address\\.ID:{2000}')

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2002, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_multi_match_after_update():
    producer = connect_redis()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    d = cct_prepare.generate_single_object(1001 , 2001, "bbb")
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{ccc}")

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{ccc} @User\\.ID:{1001}')

    client1.execute_command('CCT.FT.SEARCH' , cct_prepare.TEST_INDEX_NAME , '@User\\.PASSPORT:{aaa} @User\\.ID:{1000} @User\\.Address\\.ID:{2000}')

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1000 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_long_and_query():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    TEST_INDEX_NAME = "usersJsonIdx"
    TEST_INDEX_PREFIX = "users:"
    schema = (TagField("$.User.key1", as_name="User.key1"), TagField("$.User.key2", as_name="User.key2"),  \
              TagField("$.User.key3", as_name="User.key3"), TagField("$.User.key4", as_name="User.key4"),  \
              TagField("$.User.key5", as_name="User.key5"), TagField("$.User.key6", as_name="User.key6"),  \
              TagField("$.User.key7", as_name="User.key7"), TagField("$.User.key8", as_name="User.key8") )
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

    # ADD INITIAL DATA
    d = {}
    d["User"] = {}
    d["User"]["key1"] = "val1"
    d["User"]["key2"] = "val2"
    d["User"]["key3"] = "val3"
    d["User"]["key4"] = "val4"
    d["User"]["key5"] = "val5"
    d["User"]["key6"] = "val6"
    d["User"]["key7"] = "val7"
    d["User"]["key8"] = "val8"
    d["User"]["key9"] = "val9"

    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    res = client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.key1:{key1} ")
    print(res)

    # UPDATE DATA
    producer.json().set(key, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)
