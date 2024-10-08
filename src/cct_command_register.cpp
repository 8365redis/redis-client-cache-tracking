#include <errno.h>
#include <string.h>
#include <vector>
#include <string>
#include <unordered_map>

#include "logger.h"
#include "query_parser.h"
#include "constants.h"
#include "cct_query_tracking_data.h"
#include "json_handler.h"
#include "client_tracker.h"
#include "cct_command_register.h"


void Send_Snapshot(RedisModuleCtx *ctx, RedisModuleKey *stream_key, std::string client_name_str) {
    RedisModule_AutoMemory(ctx);

    std::string client_tracking_group = Get_Client_Client_Tracking_Group(client_name_str);
    if (client_tracking_group.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Send_Snapshot failed to get client tracking group" );
        return ;
    } 

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Send_Snapshot starts for client : " + client_name_str  + " with tracking group : " + client_tracking_group);
    // First get clients queries
    std::vector<std::string> client_queries;
    std::string client_query_key_str = CCT_MODULE_CLIENT_2_QUERY + client_tracking_group;
    RedisModuleCallReply *c2q_smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", client_query_key_str.c_str());
    const size_t reply_length = RedisModule_CallReplyLength(c2q_smembers_reply);
    for (size_t i = 0; i < reply_length; i++) {
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(c2q_smembers_reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *query_name = RedisModule_CreateStringFromCallReply(key_reply);
            const char *query_name_str = RedisModule_StringPtrLen(query_name, NULL);
            client_queries.push_back(std::string(query_name_str));
        }
    }

    // Queries that doesn't match to any key
    std::vector<std::string> empty_queries;
   
    // Second get the tracked keys from queries
    std::unordered_map<std::string, std::vector<std::string>> client_keys_2_query;
    for(const auto &query : client_queries) {
        std::string q2k_key_str = CCT_MODULE_QUERY_2_KEY + query;
        RedisModuleCallReply *q2k_smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", q2k_key_str.c_str());
        const size_t reply_length = RedisModule_CallReplyLength(q2k_smembers_reply);
        if(reply_length == 0) {
            empty_queries.push_back(query);
        }
        for (size_t i = 0; i < reply_length; i++) {
            RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(q2k_smembers_reply, i);
            if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
                RedisModuleString *key_name = RedisModule_CreateStringFromCallReply(key_reply);
                const char *key_name_str = RedisModule_StringPtrLen(key_name, NULL);
                client_keys_2_query[std::string(key_name_str)].push_back(query);
            }
        }
    }
    
    // Third get values for the key
    std::unordered_map<std::string, std::string> client_keys_2_values;
    for(const auto &pair : client_keys_2_query) { 
        std::string key = pair.first;
        std::string json_value = Get_Json_Str(ctx, key);
        client_keys_2_values[key] = json_value;
    }

    // Write to client stream   
    for (const auto &pair : client_keys_2_query) {
        std::string key = pair.first;
        auto client_queries_internal = client_keys_2_query[key];
        std::vector<std::string> client_queries_internal_original;
        for (auto q : client_queries_internal) {
            client_queries_internal_original.push_back(Normalized_to_Original(q));
        }
        std::string client_queries_internal_str;
        for(auto const& e : client_queries_internal_original) client_queries_internal_str += (e + CCT_MODULE_QUERY_DELIMETER);
        if(client_queries_internal_str.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
            client_queries_internal_str.erase(client_queries_internal_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
        }
        std::string event = "json.set";
        if(client_keys_2_values[key].empty()){
            event = "del";
            client_queries_internal_str = "";
        }
        if (Add_Event_To_Stream(ctx, client_name_str, event, key, client_keys_2_values[key], client_queries_internal_str) != REDISMODULE_OK) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Snaphot failed to adding to the stream." );
            return ;
        }
    }

    // Write empty queries to client stream  
    for (auto k : empty_queries) {
        std::string original_query = Normalized_to_Original(k);
        if (Add_Event_To_Stream(ctx, client_name_str, "json.set", "", "", original_query) != REDISMODULE_OK) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Snaphot failed to adding to the stream for empty queries." );
            return ;
        }
    }

    //Finalize stream writing with end of snapshot
    RedisModuleString **xadd_params_for_eos = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * 2);
    xadd_params_for_eos[0] = RedisModule_CreateString(ctx, CCT_MODULE_END_OF_SNAPSHOT.c_str(), CCT_MODULE_END_OF_SNAPSHOT.length());
    xadd_params_for_eos[1] = RedisModule_CreateString(ctx, CCT_MODULE_END_OF_SNAPSHOT.c_str(), CCT_MODULE_END_OF_SNAPSHOT.length());
    int stream_add_resp_eos = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params_for_eos, 1);
    RedisModule_Free(xadd_params_for_eos);
    if (stream_add_resp_eos != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Send_Snapshot failed to write end of snapshot." );
        return ;
    }
    
}

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc < 2  || argc > 4) {
        return RedisModule_WrongArity(ctx);
    }
    
    // Get Client ID
    RedisModuleString *client_name = argv[1];
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    
    // Set client name
    if (RedisModule_SetClientNameById(client_id, client_name) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client name." );
        return RedisModule_ReplyWithError(ctx, "Setting client name has failed");
    }

    std::string client_tracking_group_str = "";
    if (argc > 2) {
        RedisModuleString *client_tracking_group = argv[2];
        client_tracking_group_str = RedisModule_StringPtrLen(client_tracking_group, NULL);
    }

    if(client_tracking_group_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Register_RedisCommand client tracking group name is not given using client name : " + client_name_str );
        client_tracking_group_str = client_name_str;
    }

    Add_To_Client_Tracking_Group(client_tracking_group_str, client_name_str);

    unsigned long long client_query_ttl = 0;
    if (argc == 4) {
        RedisModuleString *client_query_ttl_str = argv[3];
        if(RedisModule_StringToULongLong(client_query_ttl_str, &client_query_ttl) == REDISMODULE_ERR ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client query TTL. Invalid TTL value." );
            return RedisModule_ReplyWithError(ctx, "Setting query TTL has failed");
        }
    }

    // Update client connection status
    Connect_Client(client_name_str);

    // Update the client TTL
    if ( Update_Client_TTL(ctx , true) == false ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set TTL.");
        return RedisModule_ReplyWithError(ctx, "Setting client TTL has failed");
    }

    // Update the client Query TTL
    if(client_query_ttl == 0 ) {
        Set_Client_Query_TTL(ctx, client_tracking_group_str, (cct_config.CCT_QUERY_TTL_SECOND_CFG * MS_MULT));
    } else {
        Set_Client_Query_TTL(ctx, client_tracking_group_str, client_query_ttl * MS_MULT); // Argument TTL is in second
    }
    
    // Check if the stream exists and delete if it is
    if( RedisModule_KeyExists(ctx, client_name) ) { // NOT checking if it is stream
        RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
        if (RedisModule_DeleteKey(stream_key) != REDISMODULE_OK ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to delete the stream." );
            return RedisModule_ReplyWithError(ctx, "Failed to delete client stream");
        }
    }

    // Create a new stream
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * 2);
    const char *dummy = "d";
    xadd_params[0] = RedisModule_CreateString(ctx, dummy, strlen(dummy));
    xadd_params[1] = RedisModule_CreateString(ctx, dummy, strlen(dummy));
    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, 1);
    RedisModule_Free(xadd_params);
    if (stream_add_resp != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
        return RedisModule_ReplyWithError(ctx, "Failed to create client stream");
    }
    RedisModule_StreamTrimByLength(stream_key, 0, 0);  // Clear the stream

    // Send SNAPSHOT to client
    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Before Send_Snapshot starts for client : " + client_name_str );
    //RedisModuleCtx *detached_ctx = RedisModule_GetDetachedThreadSafeContext(ctx);
    //std::thread snapshot_sender(Send_Snapshot, detached_ctx, stream_key, client_name_str);
    //snapshot_sender.detach();
    Send_Snapshot(ctx, stream_key, client_name_str);
    

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}