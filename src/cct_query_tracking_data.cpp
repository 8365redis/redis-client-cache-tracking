#include "cct_query_tracking_data.h"


void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_name) {
    std::string query_str = RedisModule_StringPtrLen(query, NULL);
    std::string query_term = Get_Query_Term(query_str);
    std::string query_attribute = Get_Query_Attribute(query_str);
    std::string query_tracking_key_str = CCT_MODULE_QUERY_PREFIX + query_term + CCT_MODULE_KEY_SEPERATOR + query_attribute ;
    
    RedisModuleCallReply *sadd_reply = RedisModule_Call(ctx, "SADD", "cc", query_tracking_key_str.c_str()  , client_name.c_str());
    if (RedisModule_CallReplyType(sadd_reply) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed while registering query tracking key: " +  query_tracking_key_str);
    }

    // Save the Query:Client for Tracking and Expire
    std::string query_client_key_name_str = CCT_MODULE_CLIENT_QUERY_PREFIX + query_term + CCT_MODULE_KEY_SEPERATOR + query_attribute + CCT_MODULE_KEY_SEPERATOR + client_name;
    RedisModuleString *query_client_key_name = RedisModule_CreateString(ctx, query_client_key_name_str.c_str() , query_client_key_name_str.length());
    RedisModuleString *empty = RedisModule_CreateString(ctx, "1" , 1);
    RedisModuleKey *query_client_key = RedisModule_OpenKey(ctx, query_client_key_name, REDISMODULE_WRITE);
    if(RedisModule_StringSet(query_client_key, empty) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed while registering client query tracking key: " +  query_client_key_name_str);
    }
    if(RedisModule_SetExpire(query_client_key, CCT_TTL) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed set expire for client query tracking key: " +  query_client_key_name_str);
    }
}

void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client) {
    std::string key_with_prefix = CCT_MODULE_TRACKING_PREFIX + key;
    RedisModuleCallReply *sadd_key_reply = RedisModule_Call(ctx, "SADD", "cc", key_with_prefix.c_str()  , client.c_str());
    if (RedisModule_CallReplyType(sadd_key_reply) != REDISMODULE_REPLY_INTEGER ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Tracking_Clients_From_Changed_JSON failed while registering tracking key: " +  key_with_prefix);
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON added to stream: " + key_with_prefix);
    }
    
    RedisModuleString *key_tracking_set_key_str = RedisModule_CreateString(ctx, key_with_prefix.c_str() , key_with_prefix.length());
    RedisModuleKey *key_tracking_set_key = RedisModule_OpenKey(ctx, key_tracking_set_key_str, REDISMODULE_WRITE);
    if(RedisModule_KeyType(key_tracking_set_key) == REDISMODULE_KEYTYPE_EMPTY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed set expire for client tracking set (key null): " +  key_with_prefix);
    }
    if(RedisModule_SetExpire(key_tracking_set_key, CCT_TTL) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed set expire for client tracking set: " +  key_with_prefix);
    }
}

int Add_Event_To_Stream(RedisModuleCtx *ctx, const std::string client, const std::string event, RedisModuleString * key, const std::string value, const std::string queries) { 

    RedisModuleString *client_name = RedisModule_CreateString(ctx, client.c_str(), client.length());
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * 8);
    xadd_params[0] = RedisModule_CreateString(ctx, "operation", strlen("operation"));
    std::string internal_event = CCT_KEY_EVENTS.at(event);
    xadd_params[1] = RedisModule_CreateString(ctx, internal_event.c_str(), internal_event.length());
    xadd_params[2] = RedisModule_CreateString(ctx, "key", strlen("key"));
    if(key != NULL) {
        xadd_params[3] = key;
    } else {
        xadd_params[3] = RedisModule_CreateString(ctx, "", strlen(""));
    }
    xadd_params[4] = RedisModule_CreateString(ctx, "value", strlen("value"));
    xadd_params[5] = RedisModule_CreateString(ctx, value.c_str(), value.length());
    xadd_params[6] = RedisModule_CreateString(ctx, "queries", strlen("queries"));
    xadd_params[7] = RedisModule_CreateString(ctx, queries.c_str(), queries.length());
    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, 4);
    if (stream_add_resp != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Event_To_Stream failed to add the stream." );
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
