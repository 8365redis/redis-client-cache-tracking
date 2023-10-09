#include "cct_offline_query_expire.h"

void Handle_RDB_Event(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_RDB_Event called");

    if (eid.id == REDISMODULE_EVENT_LOADING) {
        switch (subevent) {
            case REDISMODULE_SUBEVENT_LOADING_ENDED: 
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Client_Event REDISMODULE_SUBEVENT_PERSISTENCE_ENDED ");
                Handle_Offline_Query_Expire(ctx);
                break;
        }
    }
}

int Handle_Offline_Query_Expire(RedisModuleCtx *ctx) {
    
    // Get Existing Queries
    std::string pattern_existing = CCT_MODULE_QUERY_CLIENT + "*";
    RedisModuleCallReply *existing_keys_reply = RedisModule_Call(ctx, "KEYS", "c", pattern_existing.c_str());
    if (RedisModule_CallReplyType(existing_keys_reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Offline_Query_Expire failed to get keys for existing");
        return REDISMODULE_ERR;
    }
    const size_t existing_reply_length = RedisModule_CallReplyLength(existing_keys_reply);
    std::vector<std::string> existing_keys;
    for (size_t i = 0; i < existing_reply_length; i++) {   
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(existing_keys_reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *response = RedisModule_CreateStringFromCallReply(key_reply);
            const char *response_str = RedisModule_StringPtrLen(response, NULL);
            std::string plain_query(std::string(response_str).substr(CCT_MODULE_QUERY_CLIENT.length()));
            existing_keys.push_back(plain_query);
        }
    }

    //for(auto k : existing_keys) {
    //    std::cout<<"Existing Query-Client Pairs : "<<k<<std::endl;
    //}
    
    // Get Query Client Pairs From Metadata
    std::string pattern = CCT_MODULE_QUERY_2_CLIENT + "*";
    RedisModuleCallReply *q2c_keys_reply = RedisModule_Call(ctx, "KEYS", "c", pattern.c_str());
    if (RedisModule_CallReplyType(q2c_keys_reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Offline_Query_Expire failed to get keys");
        return REDISMODULE_ERR;
    }
    const size_t q2c_reply_length = RedisModule_CallReplyLength(q2c_keys_reply);
    std::vector<std::string> q2c_keys;
    for (size_t i = 0; i < q2c_reply_length; i++) {   
        RedisModuleCallReply *q2c_key_reply = RedisModule_CallReplyArrayElement(q2c_keys_reply, i);
        if (RedisModule_CallReplyType(q2c_key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *response = RedisModule_CreateStringFromCallReply(q2c_key_reply);
            const char *response_str = RedisModule_StringPtrLen(response, NULL);
            q2c_keys.push_back(response_str);
        }
    }

    std::vector<std::string> before_offline_keys;
    for( auto k : q2c_keys) {
        RedisModuleCallReply *q2c_members_reply = RedisModule_Call(ctx, "SMEMBERS", "c", k.c_str());
        if (RedisModule_CallReplyType(q2c_members_reply) != REDISMODULE_REPLY_ARRAY) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Offline_Query_Expire failed to get members");
            return REDISMODULE_ERR;
        }
        const size_t q2c_members_reply_length = RedisModule_CallReplyLength(q2c_members_reply);
        for (size_t i = 0; i < q2c_members_reply_length; i++) {   
            RedisModuleCallReply *q2c_member_reply = RedisModule_CallReplyArrayElement(q2c_members_reply, i);
            if (RedisModule_CallReplyType(q2c_member_reply) == REDISMODULE_REPLY_STRING){
                RedisModuleString *response = RedisModule_CreateStringFromCallReply(q2c_member_reply);
                const char *response_str = RedisModule_StringPtrLen(response, NULL);
                std::string query_2_client_found = k + CCT_MODULE_KEY_SEPERATOR + response_str;
                std::string plain_query_client_pair(query_2_client_found.substr(CCT_MODULE_QUERY_2_CLIENT.length()));
                before_offline_keys.push_back(plain_query_client_pair);
            }
        }        
    }

    //for(auto k : before_offline_keys) {
    //    std::cout<<"Before Offline Query-Client Pairs : "<<k<<std::endl;
    //}

    std::set<std::string> already_existing_query_client_pairs(existing_keys.begin(), existing_keys.end());
    std::set<std::string> before_offline_query_client_pairs(before_offline_keys.begin(), before_offline_keys.end());
    std::set<std::string> query_client_pairs_to_expire;
    std::set_difference (before_offline_query_client_pairs.begin(), before_offline_query_client_pairs.end(), already_existing_query_client_pairs.begin(), already_existing_query_client_pairs.end(), inserter(query_client_pairs_to_expire, query_client_pairs_to_expire.begin()));

    //for(auto k : query_client_pairs_to_expire) {
    //    std::cout<<"Expire these Query-Client Pairs : "<<k<<std::endl;
    //}

    // Expire the queries
    for(auto k : query_client_pairs_to_expire) {
        std::string query_client_pair_key_in_metadata_format(CCT_MODULE_QUERY_CLIENT + k);
        Handle_Query_Expire(ctx, query_client_pair_key_in_metadata_format);
    }

    return REDISMODULE_OK;

}