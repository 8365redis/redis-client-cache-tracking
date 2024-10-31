import redis
import pytest
from redis.commands.json.path import Path
import cct_prepare
from manage_redis import connect_redis, connect_redis_with_start, kill_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC
import time


@pytest.fixture(autouse=True)
def before_and_after_test():
    #print("Start")
    yield
    kill_redis()
    #print("End")

def test_basic_ft_aggregate_return_first():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(10):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[10, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

def test_basic_ft_aggregate_return_cached_same_client():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    time.sleep(2)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''


def test_basic_ft_aggregate_return_cached_different_clients():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(100):
        d = cct_prepare.generate_single_object(1000 + i , 2000 - i, "aaa")
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)

    # REGISTER
    client1 = connect_redis()
    resp = client1.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    client2 = connect_redis()
    resp = client2.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_2)
    assert cct_prepare.OK in str(resp)

    client3 = connect_redis()
    resp = client3.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_3)
    assert cct_prepare.OK in str(resp)

    client4 = connect_redis()
    resp = client4.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_4)
    assert cct_prepare.OK in str(resp)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client4.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    time.sleep(4)

    response = client1.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client2.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client3.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''

    response = client4.execute_command("CCT2.FT.AGGREGATE " + cct_prepare.TEST_INDEX_NAME + " * SORTBY 1 @User.ID LIMIT 0 3")
    assert str(response) == '''[100, ['User.ID', '1000'], ['User.ID', '1001'], ['User.ID', '1002']]'''



def test_ft_aggregate_return_with_different_queries():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(40):
        if i % 4 == 1:
            d = cct_prepare.generate_single_object(1000 + i , 2000 , "aaa")
        elif i % 4 == 1:
            d = cct_prepare.generate_single_object(1000 , 2000 + i, "bbb")
        elif i % 4 == 2:
            d = cct_prepare.generate_single_object(1000 , 2000 , "ccc")
        else:
            d = cct_prepare.generate_single_object(1000 -i , 2000 - i  , "ddd")   
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
    
    client = connect_redis()
    resp = client.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 0 REDUCE COUNT 0 AS total_users''' # Count the Total Number of Users
    response = client.execute_command(query)
    #print(response)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.PASSPORT REDUCE COUNT 0 AS user_count''' # Get the Number of Users per Passport
    response = client.execute_command(query)
    #print(response)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 2 @User.ID @User.PASSPORT''' # List All Users with Their Passports
    response = client.execute_command(query)
    #print(response)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx @User\\.PASSPORT:{aaa} GROUPBY 1 @User.ID''' # Find Users with a Specific Passport
    response = client.execute_command(query)
    #print(response)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.Address.ID REDUCE COUNT 0 AS user_count''' # Count Users per Address ID
    response = client.execute_command(query)
    #print(response)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.Address.ID REDUCE COUNT 0 AS user_count FILTER @user_count>5''' # Find Addresses with More Than 5 Users
    response = client.execute_command(query)
    #print(response)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.PASSPORT REDUCE COUNT 0 AS passport_count SORTBY 2 @passport_count DESC MAX 3''' # Get the Top 3 Most Common Passports
    response = client.execute_command(query)
    #print(response)
    
    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.Address.ID REDUCE COUNT 0 AS user_count GROUPBY 0 REDUCE AVG 1 @user_count AS avg_users_per_address''' # Calculate the Average Number of Users per Address
    response = client.execute_command(query)
    #print(response)
    
    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.ID REDUCE COUNT_DISTINCT 1 @User.PASSPORT AS passport_count FILTER @passport_count>1''' # Find Users with Multiple Passports
    response = client.execute_command(query)
    #print(response)
    
    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.PASSPORT''' # Retrieve All Unique Passport Numbers
    response = client.execute_command(query)
    #print(response)
    
    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 0 REDUCE COUNT_DISTINCT 1 @User.Address.ID AS distinct_addresses''' # Count Distinct Addresses
    response = client.execute_command(query)
    #print(response)
    
    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 2 @User.PASSPORT @User.Address.ID REDUCE COUNT 0 AS user_count ''' # Get Users Grouped by Passport and Address
    response = client.execute_command(query)
    #print(response)
    
    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.ID SORTBY 2 @User.ID ASC''' # Find Passports Used by Multiple Users
    response = client.execute_command(query)
    #print(response)
    
    query = '''CCT2.FT.AGGREGATE usersJsonIdx * GROUPBY 1 @User.ID SORTBY 2 @User.ID ASC''' # Sorting Users by ID
    response = client.execute_command(query)
    #print(response)
    


def test_ft_aggregate_return_with_large_dataset_1():
    r = connect_redis_with_start()
    cct_prepare.flush_db(r) # clean all db first
    cct_prepare.create_index(r)

    # ADD INITIAL DATA
    for i in range(2000):
        if i % 4 == 1:
            d = cct_prepare.generate_single_object(1000 + i , 2000 , "aaa")
        elif i % 4 == 1:
            d = cct_prepare.generate_single_object(1000 , 2000 + i, "bbb")
        elif i % 4 == 2:
            d = cct_prepare.generate_single_object(1000 , 2000 , "ccc")
        else:
            d = cct_prepare.generate_single_object(1000 -i , 2000 - i  , "ddd")   
        r.json().set(cct_prepare.TEST_INDEX_PREFIX + str(i), Path.root_path(), d)
    
    client = connect_redis()
    resp = client.execute_command("CCT2.REGISTER " + cct_prepare.TEST_APP_NAME_1)
    assert cct_prepare.OK in str(resp)

    query = '''CCT2.FT.AGGREGATE usersJsonIdx * LOAD 1 @User.ID LIMIT 0 1000''' # Get 1000 data
    response = client.execute_command(query)
    #print(response)