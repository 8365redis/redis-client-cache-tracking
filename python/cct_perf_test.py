import time
import pytest
import cct_prepare
from statistics import mean
from manage_redis import connect_redis_with_start, kill_redis
from constants import SKIP_PERF_TEST

@pytest.fixture(autouse=True)
def before_and_after_test():
    print("Start")
    yield
    kill_redis()
    print("End")

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_cct_search_latency_single_tag_search():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)
    data = cct_prepare.generate_input(10)
    cct_prepare.add_list(r, data)

    # REGISTER
    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)

    # SEARCH WITH CCT
    start_time = time.time()
    cct_resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "}")
    end_time = time.time() 
    cct_latency = (end_time - start_time) * 1000 # time.time returns ns
    # SEARCH WITH DEFAULT
    start_time = time.time()
    default_resp = r.execute_command("FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "}")
    end_time = time.time()
    default_latency = (end_time - start_time) * 1000 # time.time returns ns
    assert cct_resp == default_resp
    diff_micron = cct_latency - default_latency
    assert 1.0 > diff_micron

    print("CCT LATENCY : " + str(cct_latency))
    print("DEFAULT LATENCY : " + str(default_latency))
    print("DIFF : " + str(diff_micron))

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_cct_search_latency_multi_tag_search():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)
    data = cct_prepare.generate_input(10)
    cct_prepare.add_list(r, data)

    # REGISTER
    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    # SEARCH WITH CCT
    start_time = time.time()
    cct_resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
    end_time = time.time() 
    cct_latency = (end_time - start_time) * 1000 # time.time returns ns
    # SEARCH WITH DEFAULT
    start_time = time.time()
    default_resp =  r.execute_command("FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
    end_time = time.time()
    default_latency = (end_time - start_time) * 1000 # time.time returns ns
    assert cct_resp == default_resp
    diff_micron = cct_latency - default_latency
    assert 1.0 > diff_micron

    print("CCT LATENCY : " + str(cct_latency))
    print("DEFAULT LATENCY : " + str(default_latency))
    print("DIFF : " + str(diff_micron))

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_cct_search_latency_single_tag_search_multiple():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)
    data = cct_prepare.generate_input(10)
    cct_prepare.add_list(r, data)

    # REGISTER
    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    latencies = []
    for _ in range(100):
        # SEARCH WITH CCT
        start_time = time.time()
        cct_resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
        end_time = time.time() 
        cct_latency = (end_time - start_time) * 1000 # time.time returns ns
        # SEARCH WITH DEFAULT
        start_time = time.time()
        default_resp =  r.execute_command("FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
        end_time = time.time()
        default_latency = (end_time - start_time) * 1000 # time.time returns ns
        assert cct_resp == default_resp
        diff_micron = cct_latency - default_latency
        assert 1.0 > diff_micron
        latencies.append(diff_micron)

    print("Latency diff average : " + str(mean(latencies)))

@pytest.mark.skipif(SKIP_PERF_TEST ,
                    reason="Only run manually")
def test_cct_search_latency_multiple_tag_search_multiple():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)
    data = cct_prepare.generate_input(10)
    cct_prepare.add_list(r, data)

    # REGISTER
    r.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    
    latencies = []
    for _ in range(100):
        # SEARCH WITH CCT
        start_time = time.time()
        default_resp =  r.execute_command("FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
        end_time = time.time() 
        cct_latency = (end_time - start_time) * 1000 # time.time returns ns
        # SEARCH WITH DEFAULT
        start_time = time.time()
        cct_resp = r.execute_command("CCT2.FT.SEARCH "+ cct_prepare.TEST_INDEX_NAME +" @User\\.ID:{" + data[0]["User"]["ID"] + "\\|" + data[1]["User"]["ID"]  + "}")
        end_time = time.time()
        default_latency = (end_time - start_time) * 1000 # time.time returns ns
        assert cct_resp == default_resp
        diff_micron = cct_latency - default_latency
        assert 1.0 > diff_micron
        latencies.append(diff_micron)

    print("Latency diff average : " + str(mean(latencies)))