#include "cct_command_register.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Log_Command(ctx,argv,argc);
    
    if (argc < 2  || argc > 3) {
        return RedisModule_WrongArity(ctx);
    }

    unsigned long long client_query_ttl = 0;
    if (argc == 3) {
        RedisModuleString *client_query_ttl_str = argv[2];
        if(RedisModule_StringToULongLong(client_query_ttl_str, &client_query_ttl) == REDISMODULE_ERR ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client query TTL. Invalid TTL value." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }

    // Get Client ID
    RedisModuleString *client_name = argv[1];
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    
    // Set client name
    if (RedisModule_SetClientNameById(client_id, client_name) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    // Update client connection status
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    Connect_Client(client_name_str);

    // Update the client TTL
    if ( Update_Client_TTL(ctx , true) == false ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set TTL.");
        return RedisModule_ReplyWithError(ctx, "Setting TTL Failed");
    }

    // Update the client Query TTL
    if(client_query_ttl == 0 ) {
        Set_Client_Query_TTL(ctx, client_name_str, CCT_QUERY_TTL);
    } else {
        Set_Client_Query_TTL(ctx, client_name_str, client_query_ttl * MS_MULT); // Argument TTL is in second
    }
    
    // Check if the stream exists and delete if it is
    if( RedisModule_KeyExists(ctx, client_name) ) { // NOT checking if it is stream
        RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
        if (RedisModule_DeleteKey(stream_key) != REDISMODULE_OK ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to delete the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }

    // Create a new stream
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * 2);
    const char *dummy = "d";
    xadd_params[0] = RedisModule_CreateString(ctx, dummy, strlen(dummy));
    xadd_params[1] = RedisModule_CreateString(ctx, dummy, strlen(dummy));
    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, 1);
    if (stream_add_resp != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    RedisModule_StreamTrimByLength(stream_key, 0, 0);  // Clear the stream

    // Send SNAPSHOT to client
    // First get clients queries
    std::vector<std::string> client_queries;
    std::string client_query_key_str = CCT_MODULE_CLIENT_2_QUERY + client_name_str;
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
   
    // Second get the tracked keys from queries
    std::unordered_map<std::string, std::vector<std::string>> client_keys_2_query;
    for(const auto &query : client_queries) {
        std::string q2k_key_str = CCT_MODULE_QUERY_2_KEY + query;
        RedisModuleCallReply *q2k_smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", q2k_key_str.c_str());
        const size_t reply_length = RedisModule_CallReplyLength(q2k_smembers_reply);
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
        std::ostringstream imploded;
        std::copy(client_queries_internal.begin(), client_queries_internal.end(), std::ostream_iterator<std::string>(imploded, CCT_MODULE_QUERY_DELIMETER.c_str()));
        std::string client_queries_internal_str = imploded.str();
        if(client_queries_internal_str.length() > CCT_MODULE_QUERY_DELIMETER.length()) {
            client_queries_internal_str.erase(client_queries_internal_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
        }
        if (Add_Event_To_Stream(ctx, client_name_str, "json.set", key, client_keys_2_values[key], client_queries_internal_str) != REDISMODULE_OK) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Snaphot failed to adding to the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }

    //Finalize stream writing with end of snapshot
    RedisModuleString **xadd_params_for_eos = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * 2);
    xadd_params_for_eos[0] = RedisModule_CreateString(ctx, CCT_MODULE_END_OF_SNAPSHOT.c_str(), CCT_MODULE_END_OF_SNAPSHOT.length());
    xadd_params_for_eos[1] = RedisModule_CreateString(ctx, CCT_MODULE_END_OF_SNAPSHOT.c_str(), CCT_MODULE_END_OF_SNAPSHOT.length());
    int stream_add_resp_eos = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params_for_eos, 1);
    if (stream_add_resp_eos != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to write end of snapshot." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}