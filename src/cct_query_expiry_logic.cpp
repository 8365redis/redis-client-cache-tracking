#include "cct_query_expiry_logic.h"

int Handle_Query_Expire(RedisModuleCtx *ctx , std::string key) {
    RedisModule_AutoMemory(ctx);
    
    if (key.rfind(CCT_MODULE_KEY_SEPERATOR) == std::string::npos) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Query_Expire key : " + key  + " is invalid.");
        return REDISMODULE_ERR;
    }
    std::string client_name(key.substr(key.rfind(CCT_MODULE_KEY_SEPERATOR) + 1));
    std::string query(key.substr(0, key.rfind(CCT_MODULE_KEY_SEPERATOR)));
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Query_Expire parsed  client_name :" + client_name  + " , query :" + query);
    // FIX HERE : what():  std::out_of_range -> basic_string::substr: __pos (which is 27) > this->size() (which is 22)
    // Handle_Query_Expire parsed  client_name :1 , query :CCT:TRACKED_KEYS:users
    std::string new_query(query.substr(CCT_MODULE_QUERY_CLIENT.length()));
    std::string new_query_with_prefix = CCT_MODULE_QUERY_2_CLIENT  + new_query;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Query_Expire parsed  client_name :" + client_name  + " , new_query :" + new_query_with_prefix);

    RedisModuleCallReply *srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", new_query_with_prefix.c_str()  , client_name.c_str());
    if (RedisModule_CallReplyType(srem_key_reply) != REDISMODULE_REPLY_INTEGER){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting client: " +  client_name);
        return REDISMODULE_ERR;
    } else if ( RedisModule_CallReplyInteger(srem_key_reply) == 0 ) { 
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting client (non existing key): " +  client_name);
        return REDISMODULE_ERR;
    }

    // Add event to stream
    if (Add_Event_To_Stream(ctx, client_name, "query_expired", NULL , "", new_query) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed to adding to the stream." );
        return REDISMODULE_ERR;
    }
    
    return REDISMODULE_OK;
}