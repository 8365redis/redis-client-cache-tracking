import random
import names
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

def generate_input(count):
    data = []
    for x in range(count):
        d = {}
        d['age'] = random.randint(20, 90)
        d['name'] = names.get_first_name()
        d['lastName'] = names.get_last_name()
        data.append(d)
    return data

def add_list(r, data):
    count = 1
    for d in data:
        #print(d)
        r.json().set("user:" + str(count), Path.root_path(), d)
        count = count + 1

def create_index(r):
    schema = (NumericField("$.userId", as_name="userId"),TextField("$.name", as_name="name"), TextField("$.lastName", as_name="lastName"))
    r.ft().create_index(schema, definition=IndexDefinition(prefix=["user:"], index_type=IndexType.JSON))

