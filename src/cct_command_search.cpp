#include <errno.h>
#include <string.h>
#include <vector>
#include <string>

#include "logger.h"
#include "constants.h"
#include "query_parser.h"
#include "cct_query_tracking_data.h"
#include "cct_client_tracker.h"
#include "cct_command_search.h"
#include "cct_index_tracker.h"


int FT_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "FT_Search_RedisCommand is called");
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    if(argv[1] == NULL || argv[2] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to execute query because query is NULL.");
        return REDISMODULE_ERR;
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();

    RedisModuleString *client_name_from_argv = NULL;
    FindAndRemoveClientName(argv, &argc, &client_name_from_argv);
    std::string client_name_str;
    if(client_name_from_argv != NULL) {
        client_name_str = RedisModule_StringPtrLen(client_name_from_argv, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "FT_Search_RedisCommand CLIENTNAME is provided in argv: " + client_name_str);
    } else {
        client_name_str = client_tracker.getClientName(ctx);
    }

    if (client_tracker.isClientConnected(client_name_str) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed : Client is not registered" );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }
       
    // Forward Search
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.SEARCH", "v", argv + 1, argc - 1);
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    // Parse Search Result
    const size_t reply_length = RedisModule_CallReplyLength(reply);
    RedisModule_ReplyWithArray(ctx , reply_length);

    RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
    if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
        long long size = RedisModule_CallReplyInteger(key_int_reply);
        RedisModule_ReplyWithLongLong(ctx, size);
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to get reply size." );
        return REDISMODULE_ERR;
    }

    std::vector<std::string> key_ids;
    std::vector<std::vector<std::string>> keys;
    key_ids.reserve(reply_length);
    keys.reserve(reply_length);

    for (size_t i = 1; i < reply_length; i++) {   // Starting from 1 as first one count
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *response = RedisModule_CreateStringFromCallReply(key_reply);
            const char *response_str = RedisModule_StringPtrLen(response, NULL);
            key_ids.push_back(response_str);
            std::vector<std::string> response_vector = {response_str};
            keys.push_back(response_vector);
        }else if ( RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_ARRAY){
            size_t inner_reply_length = RedisModule_CallReplyLength(reply);
            std::vector<std::string> inner_keys;
            for (size_t i = 0; i < inner_reply_length; i++) {
                RedisModuleCallReply *inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, i);
                if (RedisModule_CallReplyType(inner_key_reply) == REDISMODULE_REPLY_STRING){
                    RedisModuleString *inner_response = RedisModule_CreateStringFromCallReply(inner_key_reply);
                    const char *inner_response_str = RedisModule_StringPtrLen(inner_response, NULL);
                    inner_keys.push_back(inner_response_str);
                }
            }
            keys.push_back(inner_keys);
        }
    }

    std::unordered_map<std::string, std::string> key_value_to_stream; 
    std::string current_key_to_stream; 
    for (const auto& it : keys) {
        if ( it.size() == 1){
            RedisModule_ReplyWithStringBuffer(ctx, it.at(0).c_str(), strlen(it.at(0).c_str()));
            current_key_to_stream = it.at(0);
        }
        else {
            if (it.size() == 2) {
                RedisModule_ReplyWithArray(ctx , 2);
                RedisModule_ReplyWithStringBuffer(ctx, it.at(0).c_str(), strlen(it.at(0).c_str()));
                RedisModule_ReplyWithStringBuffer(ctx, it.at(1).c_str(), strlen(it.at(1).c_str()));
                key_value_to_stream[current_key_to_stream] = it.at(1);
            }else {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand inner response size is not expected size : " + std::to_string(it.size()) );
            }
        }
    }

    if ( client_name_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to get client name" );
        return REDISMODULE_ERR;
    }
    std::string client_tracking_group = client_tracker.getClientClientTrackingGroup(client_name_str);
    if (client_tracking_group.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to get client tracking group" );
        return REDISMODULE_ERR;
    }

    bool is_wildcard_search = false;
    RedisModuleString *index = argv[1];
    RedisModuleString *query = argv[2];
    std::string query_str = RedisModule_StringPtrLen(query, NULL);
    if(query_str == WILDCARD_SEARCH) {
        is_wildcard_search = true;
    }
    std::string index_str = RedisModule_StringPtrLen(index, NULL);
    
    if(is_wildcard_search) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "FT_Search_RedisCommand search is wildcard.");
        Track_Index(index_str, client_tracking_group);
        // Save the Query for Tracking
        Add_Tracking_Query(ctx, query, client_tracking_group, key_ids, index_str, true);
    } else {
        // Save the Query for Tracking
        Add_Tracking_Query(ctx, query, client_tracking_group, key_ids, index_str);
    }
     
    // Add tracked keys
    for (const auto& it : key_ids) {      
        Add_Tracking_Key(ctx, it, client_tracking_group);
    }    

    // Write to other client tracking group members stream
    std::set<std::string> members = client_tracker.getClientTrackingGroupClients(client_tracking_group);
    members.erase(client_name_str); // delete self so dont send to itself
    for (auto& client : members) {
        for(auto& k_v : key_value_to_stream){
            std::string query_to_stream;
            if(is_wildcard_search){
                query_to_stream = query_str;
            } else {
                query_to_stream = Normalized_to_Original(Get_Query_Normalized(query));
            }
            std::string index_and_query = index_str + CCT_MODULE_KEY_SEPERATOR + query_to_stream;
            if( Add_Event_To_Stream(ctx, client, "query", k_v.first, k_v.second, index_and_query, cct_config.CCT_SEND_OLD_VALUE_CFG) != REDISMODULE_OK) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to adding to the stream : " + client );
            }
        }
        
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "FT_Search_RedisCommand is successfully finished");
    return REDISMODULE_OK;
}