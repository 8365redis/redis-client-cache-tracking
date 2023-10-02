import redis
import time
import pytest
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis, connect_redis_with_start_without_module
from cct_test_utils import generate_json, generate_json_scheme


@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

def test_basic_json_set_test():
    producer = connect_redis_with_start()
    cct_prepare.flush_db(producer) # clean all db first

    generate_json_scheme(producer)

    total_json_put_count = 100

    start_time = time.time()
    for num in range(1,total_json_put_count):
        d = generate_json()
    #    #print("Set Count : " + str(num))
        producer.json().set("person:" + str(num), '$' , d)
    end_time = time.time()

    json_set_latency = (int)(end_time - start_time) * 1000  # time.time returns ns

    print("Module enabled latency = " + str(json_set_latency) + "ms , for json.set count : " + str(total_json_put_count))

    kill_redis()

    producer = connect_redis_with_start_without_module()
    cct_prepare.flush_db(producer) # clean all db first

    start_time = time.time()
    for num in range(1,total_json_put_count):
        d = generate_json()
        #print("Set Count : " + str(num))
        producer.json().set("person:" + str(num), '$' , d)
    end_time = time.time()

    json_set_latency = (int)(end_time - start_time) * 1000  # time.time returns ns

    print("Module disabled latency = " + str(json_set_latency) + "ms , for json.set count : " + str(total_json_put_count))    
