#include "json_handler.h"
#include "logger.h"

RedisModuleString * Get_JSON_Value(RedisModuleCtx *ctx , std::string event_str, RedisModuleString* r_key){
    RedisModule_AutoMemory(ctx);

    RedisModuleString *value;
    std::string key = RedisModule_StringPtrLen(r_key, NULL);
    RedisModuleCallReply *json_get_reply = RedisModule_Call(ctx, "JSON.GET", "s", r_key);
    if (RedisModule_CallReplyType(json_get_reply) == REDISMODULE_REPLY_NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_JSON_Value : " + event_str  + " , key " + key + " getting the value which is gone.");
        value = RedisModule_CreateString( ctx, "" , 0 );
        return value;
    } else if (RedisModule_CallReplyType(json_get_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_JSON_Value : " + event_str  + " , key " + key + " getting the value failed.");
        return NULL;
    } else {
        value = RedisModule_CreateStringFromCallReply(json_get_reply);
        return value;
    }    
}

void Recursive_JSON_Iterate(const json& j, std::string prefix , std::vector<std::string> &keys)
{
    for(auto it = j.begin(); it != j.end(); ++it)
    {
        if (it->is_structured())
        {
            Recursive_JSON_Iterate(*it, it.key(), keys);
        }
        else
        {
            keys.push_back(it.key());
        }
    }
}
