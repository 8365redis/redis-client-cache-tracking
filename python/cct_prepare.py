import random
import string
import json
from json import JSONEncoder
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

TEST_APP_NAME_1 = "app1"
TEST_APP_NAME_2 = "app2"

DUPLICATE = "Duplicate"
OK = "OK"

TEST_INDEX_NAME = "usersJsonIdx"
TEST_INDEX_PREFIX = "users."


def flush_db(r):
    r.flushall()

def generate_object(d, id, addr_id):
    d["User"] = {}
    d["User"]["ID"] = str(id)
    first = "".join( random.choices(string.ascii_uppercase + string.digits, k=3))
    second = "".join( random.choices(string.ascii_uppercase + string.digits, k=2))
    third = "".join( random.choices(string.ascii_uppercase + string.digits, k=2))
    d["User"]["PASSPORT"] = first + "-" + second + "-" + third
    d["User"]["Address"] = {}
    d["User"]["Address"]["ID"] = str(addr_id)

def generate_input(count):
    data = []
    id = 1000
    addr_id = 2000
    for x in range(count):
        d = {}
        generate_object(d , id , addr_id)
        data.append(d)
        id = id + 1
        addr_id = addr_id + 1
    return data

def add_list(r, data):
    count = 1
    for d in data:
        r.json().set(TEST_INDEX_PREFIX + str(count), Path.root_path(), d)
        count = count + 1

def create_index(r):
    schema = (TagField("$.User.ID", as_name="User.ID"), TagField("$.User.PASSPORT", as_name="User.PASSPORT"),  \
              TagField("$.User.Address.ID", as_name="User.Address.ID"))
    r.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))

