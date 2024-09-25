import pytest
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis, start_redis
import cct_prepare
from constants import CCT_OLD
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
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # QUERY
    query_value = passport_value
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1001 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1002 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # UPDATE DATA
    d = cct_prepare.generate_single_object(1003 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_backup_key_holding_value_multi_key():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    cct_prepare.create_index(producer)

    # ADD INITIAL DATA
    passport_value = "aaa"
    d = cct_prepare.generate_single_object(1000 , 2000, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(1)
    producer.json().set(key, Path.root_path(), d)
    
    d = cct_prepare.generate_single_object(1001 , 2001, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(2)
    producer.json().set(key, Path.root_path(), d)

    # FIRST CLIENT
    client1 = connect_redis()
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # QUERY
    query_value = passport_value
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # UPDATE DATA KEY2
    d = cct_prepare.generate_single_object(1001 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # UPDATE DATA KEY2
    d = cct_prepare.generate_single_object(1001 , 2000, "aaa")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_old_key_holding_value_after_delete():
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
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # QUERY
    query_value = passport_value
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1001 , 2000, query_value)
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    print(old_val)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # DELETE DATA 
    producer.delete(key)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_old_key_holding_value_after_key_no_longer_interested():
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
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # QUERY
    query_value = passport_value
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1001 , 2001, query_value)
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    print(old_val)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)


    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1001 , 2001, "bbb")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1002 , 2002, "bbb")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

def test_old_key_holding_value_after_delete_2_clients():
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
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # SECOND CLIENT
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)

    # QUERY
    query_value = passport_value
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1001 , 2000, query_value)
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    print(old_val)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

    # DELETE DATA 
    producer.delete(key)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)

def test_old_key_holding_value_after_key_no_longer_interested_only_one_client():
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
    client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # SECOND CLIENT
    client2 = connect_redis()
    client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)

    # QUERY
    query_value = passport_value
    client1.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.PASSPORT:{" + query_value + "}")
    client2.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + str(1000) + "}")

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None

    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1000 , 2001, query_value)
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    print(old_val)
    old_val_dict = eval(old_val)
    assert old_val_dict["User"]["ID"] == d["User"]["ID"]

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print(from_stream)


    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1000 , 2001, "bbb")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val != None

    # Check stream 
    from_stream = client1.xread( streams={cct_prepare.TEST_APP_NAME_1:0} )
    print("Client1 stream :")
    print(from_stream)

    # Check stream 
    from_stream = client2.xread( streams={cct_prepare.TEST_APP_NAME_2:0} )
    print("Client2 stream :")
    print(from_stream)

    # UPDATE DATA 
    d = cct_prepare.generate_single_object(1002 , 2002, "bbb")
    producer.json().set(key, Path.root_path(), d)

    # CHECK OLD VAL
    old_val_key = CCT_OLD + key
    old_val = producer.get(old_val_key)
    assert old_val == None