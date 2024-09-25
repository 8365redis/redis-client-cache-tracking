import pytest
import random
import string
import time
from redis.commands.json.path import Path
from manage_redis import kill_redis, connect_redis_with_start, connect_redis, connect_redis_with_start_without_module
import cct_prepare
from constants import SKIP_PERF_TEST
from redis.commands.search.field import TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import threading

INDEXED_KEY_LENGTH = 3
INDEXED_KEY_COUNT = 3
INITIAL_TOTAL_DATA_COUNT = 1000000
DATA_KEY_COUNT = 100

HB_INTERVAL = 120
SEARCH_COUNT = 100000

TEST_INDEX_NAME = "usersJsonIdx"
TEST_INDEX_PREFIX = "users:"
BIG_STRING = "VGbVufdqNQNXIeWo2lgmqCmnaipxK9OExlIDuKlSCB3CnWaiMQ"

def send_hb(client):
    client.execute_command("CCT2.HEARTBEAT")
    hb_sender = threading.Timer(120,send_hb, client)
    hb_sender.start()


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")

def test_snapshot_after_lots_of_queries():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first
    
    # ADD INDEX
    schema = (TagField("$.User.key0", as_name="User.key0"), TagField("$.User.key1", as_name="User.key1"),  \
              TagField("$.User.key2", as_name="User.key2"), TagField("$.User.key3", as_name="User.key3"))
    producer.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))
    
    # ADD INITIAL DATA
    start_time = time.time()
    for i in range(INITIAL_TOTAL_DATA_COUNT):
        data = {}
        data["User"] = {}
        for x in range(DATA_KEY_COUNT):
            key = "key" + str(x)
            if(x <= INDEXED_KEY_COUNT):
                data["User"][key] = ''.join(random.choices(string.digits, k=INDEXED_KEY_LENGTH))
            else:
                data["User"][key] = BIG_STRING
        producer.json().set(TEST_INDEX_PREFIX + str(i), Path.root_path(), data)
    end_time = time.time()
    initial_data_set_latency = (end_time - start_time) * 1000 # time.time returns ns
    print("initial_data_set_latency:" + str(initial_data_set_latency) )


    # REGISTER CLIENT
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    # SEND HB WITH INTERVAL
    hb_sender = threading.Timer(HB_INTERVAL,send_hb, client1)
    hb_sender.start()

    # SEND QUERIES
    start_time = time.time()
    for _ in range(SEARCH_COUNT):
        index = "key"+ str(random.randint(0,INDEXED_KEY_COUNT))
        query_value = ''.join(random.choices(string.digits, k=INDEXED_KEY_LENGTH))
        client1.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\." + index + ":{" + query_value + "}")
    end_time = time.time()
    initial_query_latency = (end_time - start_time) * 1000 # time.time returns ns
    print("initial_query_latency:" + str(initial_query_latency) )

    # DISCONNECT
    client1.connection_pool.disconnect()

    # RE-REGISTER
    client1 = connect_redis()
    start_time = time.time()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    end_time = time.time()
    register_total_time = (end_time - start_time) * 1000 # time.time returns ns
    print("register_total_time:" + str(register_total_time) )
    print(str(resp))

    res = client1.execute_command("INFO latencystats")
    print(str(res))
