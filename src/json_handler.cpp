#include "json_handler.h"
#include "logger.h"

RedisModuleString * Get_JSON_Value(RedisModuleCtx *ctx, std::string event_str, RedisModuleString* r_key){
    RedisModule_AutoMemory(ctx);

    RedisModuleString *value;
    std::string key = RedisModule_StringPtrLen(r_key, NULL);
    RedisModuleCallReply *json_get_reply = RedisModule_Call(ctx, "JSON.GET", "s", r_key);
    if (RedisModule_CallReplyType(json_get_reply) == REDISMODULE_REPLY_NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_JSON_Value : " + event_str  + " , key " + key + " getting the value which is gone.");
        value = RedisModule_CreateString( ctx, "" , 0 );
        return value;
    } else if (RedisModule_CallReplyType(json_get_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_JSON_Value : " + event_str  + " , key " + key + " getting the value failed. Type : " + std::to_string(RedisModule_CallReplyType(json_get_reply)));
        return NULL;
    } else {
        value = RedisModule_CreateStringFromCallReply(json_get_reply);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_JSON_Value : " + event_str  + " , key " + key + " is success");
        return value;
    }
}

json Get_JSON_Object(RedisModuleCtx *ctx, std::string str){
    if (!json::accept(str)){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_JSON_Object : " + str  + " is not a valid JSON");
        return NULL;
    }

    return json::parse(str);
}

void Recursive_JSON_Iterate(const json& j, std::string prefix , std::vector<std::string> &keys){

    for(auto it = j.begin(); it != j.end(); ++it)
    {
        if (it->is_structured())
        {
            std::string new_prefix;
            if( prefix.empty() == false ){
                new_prefix = prefix + CCT_MODULE_KEY_LEVEL_WITH_ESCAPE + it.key();
            } else{
                new_prefix = std::string(it.key());
            }
            Recursive_JSON_Iterate(*it, new_prefix, keys);
        }
        else
        {
            std::string value_str;
            const json::value_t val_type = it.value().type();
            if (val_type == json::value_t::number_integer || val_type == json::value_t::number_unsigned){
                value_str = std::to_string(it.value().get<int>());
            } else if (val_type == json::value_t::number_float ) {
                value_str = std::to_string(it.value().get<float>());
            } else {
                value_str = it.value().get<std::string>();
            }
            std::string new_prefix = prefix + CCT_MODULE_KEY_LEVEL_WITH_ESCAPE + it.key() + CCT_MODULE_KEY_SEPERATOR + value_str;
            keys.push_back(new_prefix);
        }
    }
}
