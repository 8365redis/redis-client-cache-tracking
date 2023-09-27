import threading
from manage_redis import connect_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC, \
                CCT_MODULE_PREFIX, CCT_HEART_BEAT_INTERVAL

def check_query_meta_data(producer , app_name , query , key , assert_list):
    assert (6 == len(assert_list))
    assert (app_name)
    assert (query)
    assert (key)
    assert (producer)

    result = producer.sismember(CCT_C2Q +  app_name,  query)
    assert ( result == assert_list[0] )
    result = producer.sismember(CCT_K2C + key, app_name)
    assert ( result == assert_list[1] )
    result = producer.sismember(CCT_K2Q + key, query)
    assert ( result == assert_list[2] )
    result = producer.sismember(CCT_Q2C + query , app_name)
    assert ( result == assert_list[3] )
    result = producer.sismember(CCT_Q2K + query , key)
    assert ( result == assert_list[4] )
    result = producer.exists(CCT_QC + query + CCT_DELI + app_name)
    assert ( result == assert_list[5] )

def get_redis_snapshot():
    print("=======REDIS SNAPSHOT BEGIN========")
    client = connect_redis()
    all_keys = client.keys("*")
    for key in all_keys:
        if CCT_MODULE_PREFIX not in key :
            continue
        if CCT_QC in key : 
            print(key + "=" + client.get(key))
        else:
            print(key + "=" + str(client.smembers(key)))
    print("========REDIS SNAPSHOT END=========")

