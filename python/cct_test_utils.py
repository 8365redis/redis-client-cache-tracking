import json
from manage_redis import connect_redis
from constants import CCT_Q2C, CCT_K2C, CCT_C2Q, \
                CCT_K2Q, CCT_DELI, CCT_Q2K, CCT_QC, \
                CCT_MODULE_PREFIX

from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

from mimesis import Field, Fieldset, Schema
from mimesis.enums import Gender, TimestampFormat
from mimesis.locales import Locale


TEST_INDEX_NAME = "usersJsonIdx"
TEST_INDEX_PREFIX = "users:"

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

def generate_json():
    field = Field(locale=Locale.EN)
    fieldset = Fieldset(locale=Locale.EN)

    schema = Schema(
        schema=lambda: {
            "pk": field("increment"),
            "uid": field("uuid"),
            "name": field("text.word"),
            "version": field("version", pre_release=True),
            "timestamp": field("timestamp", fmt=TimestampFormat.POSIX),
            "owner": {
                "email": field("person.email", domains=["mimesis.name"]),
                "token": field("token_hex"),
                "creator": field("full_name", gender=Gender.FEMALE),
            },
            "sub1-owner": {
                "email": field("person.email", domains=["mimesis.name"]),
                "token": field("token_hex"),
                "creator": field("full_name", gender=Gender.FEMALE),
            },
            "sub2-owner": {
                "email": field("person.email", domains=["mimesis.name"]),
                "token": field("token_hex"),
                "creator": field("full_name", gender=Gender.FEMALE),
            },                     
            "apps": fieldset(
                "text.word", i=20, key=lambda name: {"name": name, "id": field("uuid")}
            ),
        },
        iterations=2,
    )
    val =  schema.create() 
    return val[0]

def generate_json_scheme(r):
    schema = (TagField("$.name", as_name="name"), TagField("$.version", as_name="version"),  \
              TagField("$.timestamp", as_name="timestamp"))
    r.ft(TEST_INDEX_NAME).create_index(schema, definition=IndexDefinition(prefix=[TEST_INDEX_PREFIX], index_type=IndexType.JSON))
