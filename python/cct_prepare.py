import random
import names
import json
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

TEST_APP_NAME_1 = "app1"

TEST_INDEX_NAME = "user"
TEST_INDEX_PREFIX = "user:"

TEST_FIELD_NAME = "name"
TEST_FIELD_LAST_NAME = "lastName"
TEST_FIELD_AGE = "age"

class User:
    def __init__(self, age, name, lastName):
        self.age = age
        self.name = name
        self.lastName = lastName

def flush_db(r):
    r.flushall()

def generate_input(count):
    data = []
    for x in range(count):
        d = User (random.randint(20, 90),names.get_first_name(),names.get_last_name())
        data.append(d)
    return data

def add_list(r, data):
    count = 1
    for d in data:
        r.json().set(TEST_INDEX_PREFIX + str(count), Path.root_path(), d.__dict__)
        count = count + 1

def create_index(r):
    schema = (NumericField("$.age", as_name="age"),TextField("$.name", as_name="name"), TextField("$.lastName", as_name="lastName"))
    r.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

