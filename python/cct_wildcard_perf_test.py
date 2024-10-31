import time
import pytest
import cct_prepare
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC, I2C, CCT_EOS
from constants import SKIP_PERF_TEST


@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_wildcard_with_many_clients_many_key():
    r = connect_redis_with_start()
    time.sleep(1)
    cct_prepare.flush_db(r) # clean all db first
    time.sleep(1)
    cct_prepare.create_index(r)

    total_key_cnt = 10000
    passport_value = "aaa"
    start_time = time.time()
    for i in range(total_key_cnt):
        d = cct_prepare.generate_single_object(1000000 - i  , 3000000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        r.json().set(key, Path.root_path(), d)
    end_time = time.time()
    data_write_duration = (end_time - start_time) 
    #print("Data write duration:" + str(data_write_duration) )

    r.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")

    client_cnt = 1000
    clients = []
    start_time = time.time()
    for i in range(client_cnt):
        client = connect_redis()
        clients.append(client)
        client.execute_command("CCT2.REGISTER " + ("app" + str(i) + " ") + cct_prepare.TEST_GROUP_NAME_1 )
    end_time = time.time()
    register_duration = (end_time - start_time) 
    #print("Register duration:" + str(register_duration) )

    # SEARCH
    start_time = time.time()
    clients[0].execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " * LIMIT 0 " + str(total_key_cnt))
    end_time = time.time()
    stream_write_duration = (end_time - start_time)
    #print("CCT2.FT.SEARCH duration :" + str(stream_write_duration) )

    #from_stream = r.xread(streams={cct_prepare.TEST_APP_NAME_1:0} )
    #print(str(from_stream))


@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_wildcard_with_many_clients_many_key_data_change():
    r = connect_redis_with_start()
    time.sleep(1)
    cct_prepare.flush_db(r) # clean all db first
    time.sleep(1)
    cct_prepare.create_index(r)

    total_key_cnt = 100
    passport_value = "aaa"
    start_time = time.time()
    for i in range(total_key_cnt):
        d = cct_prepare.generate_single_object(1000000 - i  , 3000000 + i, passport_value)
        key = cct_prepare.TEST_INDEX_PREFIX + str(i)
        r.json().set(key, Path.root_path(), d)
    end_time = time.time()
    data_write_duration = (end_time - start_time) 
    #print("Data write duration:" + str(data_write_duration) )

    r.execute_command("FT.CONFIG SET MAXSEARCHRESULTS 1000000")

    client_cnt = 1000
    clients = []
    start_time = time.time()
    for i in range(client_cnt):
        client = connect_redis()
        clients.append(client)
        client.execute_command("CCT2.REGISTER " + ("app" + str(i) + " ") + cct_prepare.TEST_GROUP_NAME_1 )
    end_time = time.time()
    register_duration = (end_time - start_time) 
    #print("Register duration:" + str(register_duration) )

    # SEARCH
    start_time = time.time()
    clients[0].execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME + " * LIMIT 0 " + str(total_key_cnt))
    end_time = time.time()
    stream_write_duration = (end_time - start_time)
    #print("CCT2.FT.SEARCH duration :" + str(stream_write_duration) )

    clients[0].xtrim("app0" , 0)

    # NEW DATA
    d = cct_prepare.generate_single_object(1000000  , 3000000, passport_value)
    key = cct_prepare.TEST_INDEX_PREFIX + str(500000)
    r.json().set(key, Path.root_path(), d)

    start_time = time.time()
    from_stream = clients[0].xread(streams={"app0":0} )
    end_time = time.time()
    stream_read_duration = (end_time - start_time)
    #print("Stream read duration :" + str(stream_read_duration) )
    #print(str(from_stream))
    