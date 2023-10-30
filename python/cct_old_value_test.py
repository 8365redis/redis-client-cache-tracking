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

def test_backup_key_holding_value():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # QUERY
    query_value = passport_value
    client1.execute_command("CCT.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)