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

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

INDEXED_KEY_LENGTH = 3
INDEXED_KEY_COUNT = 3
INITIAL_TOTAL_DATA_COUNT = 100
DATA_KEY_COUNT = 100
TOTAL_CLIENT_COUNT = 10
TEST_DURATION = 15
PER_TURN_SET_COUNT = 100

HB_INTERVAL = 120
SEARCH_INTERVAL = 5
SET_INTERVAL = 2

TEST_INDEX_NAME = "usersJsonIdx"
TEST_INDEX_PREFIX = "users:"
BIG_STRING = "VGbVufdqNQNXIeWo2lgmqCmnaipxK9OExlIDuKlSCB3CnWaiMQ"

def send_hb(clients):
    for c in clients:
        c.execute_command("CCT2.HEARTBEAT")
    hb_sender = threading.Timer(120,send_hb, [clients])
    hb_sender.start()


def send_cct_search(clients):
    for c in clients:
        index = "key"+ str(random.randint(0,INDEXED_KEY_COUNT))
        query_value = ''.join(random.choices(string.digits, k=INDEXED_KEY_LENGTH))
        res = c.execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\." + index + ":{" + query_value + "}")
    search_sender = threading.Timer(SEARCH_INTERVAL, send_cct_search, [clients])
    search_sender.start()

def send_normal_search(clients):
    for c in clients:
        index = "key"+ str(random.randint(0,INDEXED_KEY_COUNT))
        query_value = ''.join(random.choices(string.digits, k=INDEXED_KEY_LENGTH))
        res = c.execute_command("FT.SEARCH "+ TEST_INDEX_NAME +" @User\\." + index + ":{" + query_value + "}")
    search_sender = threading.Timer(SEARCH_INTERVAL, send_normal_search, [clients])
    search_sender.start()

def update_data(client):
    for i in range(PER_TURN_SET_COUNT):
        index_id = str(random.randint(0,INITIAL_TOTAL_DATA_COUNT))
        data = {}
        data["User"] = {}
        for x in range(DATA_KEY_COUNT):
            key = "key" + str(x)
            if(x <= INDEXED_KEY_COUNT):
                data["User"][key] = ''.join(random.choices(string.digits, k=INDEXED_KEY_LENGTH))
            else:
                data["User"][key] = BIG_STRING
        client.json().set(TEST_INDEX_PREFIX + index_id, Path.root_path(), data)
    update_data_scheduler = threading.Timer(SET_INTERVAL, update_data, client)
    update_data_scheduler.start()
 

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_search_with_module_while_no_set():
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

    # ADD INITIAL CLIENTS
    clients = []
    for _ in range(TOTAL_CLIENT_COUNT):
        clients.append(connect_redis())
    

    # ADD INITIAL REGISTERS
    client_prefix = "client"
    client_no = 0
    for c in clients:
        client_name = client_prefix + str(client_no)
        c.execute_command("CCT2.REGISTER " + client_name )
        client_no = client_no + 1

    # SEND HB WITH INTERVAL
    hb_sender = threading.Timer(HB_INTERVAL,send_hb, [clients])
    hb_sender.start()

    # SEND SEARCH WITH INTERVAL
    search_sender = threading.Timer(SEARCH_INTERVAL,send_cct_search, [clients])
    search_sender.start()
   
    time.sleep(TEST_DURATION)

    res = clients[0].execute_command("INFO latencystats")
    print(str(res))
    
    search_sender.cancel()
    hb_sender.cancel()

    # CCT_SEARCH
    start_time = time.time()
    query_value = "non_existing_key"
    clients[0].execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.key0:{" + query_value + "}")
    end_time = time.time()
    cct_search_latency = (end_time - start_time) * 1000 # time.time returns ns
    print("cct_search_latency:" + str(cct_search_latency) )

    # FT_SEARCH
    start_time = time.time()
    clients[0].execute_command("FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.key0:{" + query_value + "}")
    end_time = time.time()
    ft_search_latency = (end_time - start_time) * 1000 # time.time returns ns
    print("ft_search_latency:" + str(ft_search_latency) )

    
@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_search_without_module_while_no_set():
    producer = connect_redis_with_start_without_module()
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

    # ADD INITIAL CLIENTS
    clients = []
    for _ in range(TOTAL_CLIENT_COUNT):
        clients.append(connect_redis())
    

    # SEND SEARCH WITH INTERVAL
    search_sender = threading.Timer(SEARCH_INTERVAL,send_normal_search, [clients])
    search_sender.start()
   
    time.sleep(TEST_DURATION)

    res = clients[0].execute_command("INFO latencystats")
    print(str(res))
    
    search_sender.cancel()


@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_search_with_module_with_set():
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

    # ADD INITIAL CLIENTS
    clients = []
    for _ in range(TOTAL_CLIENT_COUNT):
        clients.append(connect_redis())
    

    # ADD INITIAL REGISTERS
    client_prefix = "client"
    client_no = 0
    for c in clients:
        client_name = client_prefix + str(client_no)
        c.execute_command("CCT2.REGISTER " + client_name )
        client_no = client_no + 1

    # SEND HB WITH INTERVAL
    hb_sender = threading.Timer(HB_INTERVAL,send_hb, [clients])
    hb_sender.start()

    # SEND SEARCH WITH INTERVAL
    search_sender = threading.Timer(SEARCH_INTERVAL,send_cct_search, [clients])
    search_sender.start()

    # SEND UPDATES WITH INTERVAL
    update_data_scheduler = threading.Timer(SET_INTERVAL, update_data, producer)
    update_data_scheduler.start()
   
    time.sleep(TEST_DURATION)

    res = clients[0].execute_command("INFO latencystats")
    print(str(res))
    
    search_sender.cancel()
    hb_sender.cancel()
    update_data_scheduler.cancel()

    # CCT_SEARCH
    start_time = time.time()
    query_value = "non_existing_key"
    clients[0].execute_command("CCT2.FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.key0:{" + query_value + "}")
    end_time = time.time()
    cct_search_latency = (end_time - start_time) * 1000 # time.time returns ns
    print("cct_search_latency:" + str(cct_search_latency) )

    # FT_SEARCH
    start_time = time.time()
    clients[0].execute_command("FT.SEARCH "+ TEST_INDEX_NAME +" @User\\.key0:{" + query_value + "}")
    end_time = time.time()
    ft_search_latency = (end_time - start_time) * 1000 # time.time returns ns
    print("ft_search_latency:" + str(ft_search_latency) )
