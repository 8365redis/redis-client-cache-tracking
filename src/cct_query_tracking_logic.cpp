#include "cct_query_tracking_logic.h"


int Get_Tracking_Clients_From_Changed_JSON(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key,
                                             std::vector<std::string> &clients_to_update, std::string &json_str, 
                                             std::unordered_map<std::string, std::vector<std::string>> &client_to_queries_map ) {
    RedisModule_AutoMemory(ctx);

    std::string key_str = RedisModule_StringPtrLen(r_key, NULL);
    
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON :  key " + key_str + " for event: " + event);

    RedisModuleString *value = Get_JSON_Value(ctx, "" , r_key);
    if (value == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Tracking_Clients_From_Changed_JSON failed while getting JSON value for key: " +  key_str);
        return REDISMODULE_ERR;
    }

    json_str = RedisModule_StringPtrLen(value, NULL);
    if ( json_str.empty() ){ 
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON :  JSON value is empty for key: " + key_str);
        return REDISMODULE_OK;
    }

    std::vector<std::string> queries;
    nlohmann::json json_object = Get_JSON_Object(ctx, json_str);
    if(json_object == NULL){
        return REDISMODULE_ERR;
    }
    Recursive_JSON_Iterate(json_object , "", queries);

    for (auto & q : queries) {
        std::string query_with_prefix = CCT_MODULE_QUERY_2_CLIENT + q;
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON check this query for tracking: " + query_with_prefix);
        RedisModuleCallReply *smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", query_with_prefix.c_str());
        if (RedisModule_CallReplyType(smembers_reply) != REDISMODULE_REPLY_ARRAY ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Tracking_Clients_From_Changed_JSON failed while getting client names for query: " +  query_with_prefix);
            return REDISMODULE_ERR;
        } else {
            const size_t reply_length = RedisModule_CallReplyLength(smembers_reply);
            for (size_t i = 0; i < reply_length; i++) {
                RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(smembers_reply, i);
                if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
                    RedisModuleString *client = RedisModule_CreateStringFromCallReply(key_reply);
                    const char *client_str = RedisModule_StringPtrLen(client, NULL);
                    clients_to_update.push_back(std::string(client_str));
                    client_to_queries_map[std::string(client_str)].push_back(q);
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON query matched to this client(app): " + (std::string)client_str);
                    Add_Tracking_Key(ctx, key_str, client_str);
                }
            }
        }
    }
    return REDISMODULE_OK;
}

int Query_Track_Check(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key, std::vector<std::string> already_tracking_clients) {
    RedisModule_AutoMemory(ctx);

    std::vector<std::string> clients_to_update;
    std::string json_str;
    std::unordered_map<std::string, std::vector<std::string>> client_to_queries_map;
    Get_Tracking_Clients_From_Changed_JSON(ctx , event,  r_key , clients_to_update , json_str, client_to_queries_map);

    std::string key_str = RedisModule_StringPtrLen(r_key, NULL);

    std::set<std::string> already_tracking_clients_set(already_tracking_clients.begin(), already_tracking_clients.end());
    std::set<std::string> clients_to_update_set(clients_to_update.begin(), clients_to_update.end());

    std::set<std::string> total_clients;
    std::set_union(std::begin(already_tracking_clients_set), std::end(already_tracking_clients_set), std::begin(clients_to_update_set), std::end(clients_to_update_set), std::inserter(total_clients, std::begin(total_clients)));    
    // Write to stream
    for (auto & client_name : total_clients) {
        auto client_queries = client_to_queries_map[client_name];
        std::string client_queries_str;
        for(auto const& e : client_queries) client_queries_str += (e + CCT_MODULE_QUERY_DELIMETER);
        if(client_queries_str.length() > CCT_MODULE_QUERY_DELIMETER.length()){
            client_queries_str.erase(client_queries_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
        }
        if (Add_Event_To_Stream(ctx, client_name, event, key_str, json_str, client_queries_str) != REDISMODULE_OK) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed to adding to the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }
    // Now delete the tracked keys which are not matching to our queries anymore
    std::set<std::string> diff_clients;
    // already_tracking_clients_set - clients_to_update_set
    std::set_difference (already_tracking_clients_set.begin(), already_tracking_clients_set.end(), clients_to_update_set.begin(), clients_to_update_set.end(), inserter(diff_clients, diff_clients.begin()));
    // Delete no more tracked keys
    for (const auto& it : diff_clients) {
        std::string key_with_prefix = CCT_MODULE_KEY_2_CLIENT + key_str;
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check will delete no more interested client: " + it + " from tracked key : " +  key_with_prefix);
        RedisModuleCallReply *srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", key_with_prefix.c_str()  , it.c_str());
        if (RedisModule_CallReplyType(srem_key_reply) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting tracking key: " +  key_with_prefix);
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

int Notify_Callback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    RedisModule_AutoMemory(ctx);

    std::string event_str = event;
    std::string key_str = RedisModule_StringPtrLen(key, NULL);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str);

    if( CCT_KEY_EVENTS.count(event) == 0 )
    {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " not interested event." );
        return REDISMODULE_OK;        
    }

    // Ignore our self events
    if (key_str.find(CCT_MODULE_PREFIX, 0) == 0) {
        if(strcasecmp(event, "expired") != 0) {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
            return REDISMODULE_OK;
        } else if(key_str.find(CCT_MODULE_QUERY_CLIENT, 0) == 0) {
            return Handle_Query_Expire(ctx, key_str);           
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
            return REDISMODULE_OK;
        }
    }
   
    // Add prefix
    std::string key_with_prefix = CCT_MODULE_KEY_2_CLIENT + key_str;


    // First check which clients are tracking updated key
    std::vector<std::string> already_tracking_clients; 
    RedisModuleCallReply *smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", key_with_prefix.c_str());
    const size_t reply_length = RedisModule_CallReplyLength(smembers_reply);
    for (size_t i = 0; i < reply_length; i++) {
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(smembers_reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *stream_name = RedisModule_CreateStringFromCallReply(key_reply);
            const char *stream_name_str = RedisModule_StringPtrLen(stream_name, NULL);
            already_tracking_clients.push_back(std::string(stream_name_str));
        }
    }

    Query_Track_Check(ctx, event_str, key, already_tracking_clients);
    
    return REDISMODULE_OK;
}