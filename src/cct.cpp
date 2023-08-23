#include "redismodule.h"
#include "constants.h"
#include "query_parser.h"
#include "json_handler.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <iterator>
#include <algorithm> 


#ifdef __cplusplus
extern "C" {
#endif

int Query_Track_Check(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key){
    RedisModule_AutoMemory(ctx);

    std::string key = RedisModule_StringPtrLen(r_key, NULL);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check : " + event  + " , key " + key);
    if (strcasecmp(event.c_str(), "json.set") == 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check , it is json.set event: " + event  + " , key " + key);
        RedisModuleString *value = Get_JSON_Value(ctx, event , r_key);
        std::string json_str = RedisModule_StringPtrLen(value, NULL);
        /*
        json json_obj = json::parse(json_str);
        std::cout<<json_obj<<std::endl;
        for (auto it = json_obj.begin(); it != json_obj.end(); ++it)
        {
            std::cout << "key: " << it.key() << ", value:" << it.value() << '\n';
        }
        */
        return REDISMODULE_OK;
    }else {
        return REDISMODULE_OK;
    }
}

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    RedisModule_AutoMemory(ctx);

    std::string event_str = event;
    std::string key_str = RedisModule_StringPtrLen(key, NULL);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str);

    // Ignore our self events
    if (key_str.rfind(CCT_MODULE_PREFIX, 0) == 0){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
        return REDISMODULE_OK;
    }

    // Add prefix
    std::stringstream prefix_stream;
    prefix_stream<<CCT_MODULE_PREFIX<<key_str;
    std::string key_with_prefix = prefix_stream.str(); 

    // Check if the key is tracked
    RedisModuleCallReply *get_reply = RedisModule_Call(ctx, "GET", "c", key_with_prefix.c_str());
    if (RedisModule_CallReplyType(get_reply) == REDISMODULE_REPLY_NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " is not tracked : " + key_with_prefix);
        Query_Track_Check(ctx, event_str, key);
        return REDISMODULE_OK;
    } else if (RedisModule_CallReplyType(get_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "NotifyCallback event : " + event_str  + " , key " + key_str + " getting the stream key failed.");
        return REDISMODULE_ERR;
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " is tracked.");
    RedisModuleString *stream_name = RedisModule_CreateStringFromCallReply(get_reply);
    
    RedisModuleString *value = Get_JSON_Value(ctx, event_str , key);

    // Write to stream
    RedisModuleCallReply *xadd_reply =  RedisModule_Call(ctx, "XADD", "sccs", stream_name , "*", key_str.c_str() , value);
    if (RedisModule_CallReplyType(xadd_reply) != REDISMODULE_REPLY_STRING) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    return REDISMODULE_OK;
}

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Log_Command(ctx,argv,argc);
    
    if (argc != 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    // Register client
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    std::string client_id_key_str = CCT_MODULE_CLIENT_PREFIX + std::to_string(client_id);
    RedisModuleKey *client_id_key = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, client_id_key_str.c_str() , client_id_key_str.length()) ,REDISMODULE_READ | REDISMODULE_WRITE );
    if(RedisModule_KeyType(client_id_key) == REDISMODULE_KEYTYPE_STRING){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand ignored with duplicate call. Client ID : " +  std::to_string(client_id));
        return RedisModule_ReplyWithError(ctx, "Duplicate Register");
    }else { 
        if (RedisModule_StringSet(client_id_key, argv[1] ) != REDISMODULE_OK ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed while registering the client id : " +  std::to_string(client_id));
            return RedisModule_ReplyWithError(ctx, "Register Failed");
        }
    }
    
    // Set client name
    if (RedisModule_SetClientNameById(client_id, argv[1]) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    // Check if the stream exists
    RedisModuleCallReply *exists_reply = RedisModule_Call(ctx, "EXISTS", "s", argv[1]);
    if (RedisModule_CallReplyType(exists_reply) != REDISMODULE_REPLY_INTEGER) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to detect the stream." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    int stream_count = RedisModule_CallReplyInteger(exists_reply);

    // If it exists delete it
    if ( stream_count > 0 ){
        RedisModuleCallReply *del_reply = RedisModule_Call(ctx, "DEL", "s", argv[1]);
        if (RedisModule_CallReplyType(del_reply) != REDISMODULE_REPLY_INTEGER) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to delete the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }
    
    // Subscribe to key space events
    if ( RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_SET | REDISMODULE_NOTIFY_STRING |
            REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_NEW | REDISMODULE_NOTIFY_MODULE ,
             NotifyCallback) != REDISMODULE_OK ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to SubscribeToKeyspaceEvents." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));   
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int FT_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Log_Command(ctx,argv,argc);
    
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
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

    for (const auto& it : keys) {
        if ( it.size() == 1){
            RedisModule_ReplyWithStringBuffer(ctx, it.at(0).c_str(), strlen(it.at(0).c_str()));
        }
        else {
            RedisModule_ReplyWithArray(ctx , 2);
            RedisModule_ReplyWithStringBuffer(ctx, it.at(0).c_str(), strlen(it.at(0).c_str()));
            RedisModule_ReplyWithStringBuffer(ctx, it.at(1).c_str(), strlen(it.at(1).c_str()));
        }
    }

    // Get client name
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id); 
    if ( client_name == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to get client name." );
        return REDISMODULE_ERR;
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if ( client_name_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed because client name is not set." );
        return REDISMODULE_ERR;
    }
    
    // Add tracked keys
    for (const auto& it : key_ids) {
        std::string key_with_prefix = CCT_MODULE_PREFIX + it; 
        RedisModuleCallReply *set_reply = RedisModule_Call(ctx, "SET", "cc", key_with_prefix.c_str()  , client_name_str.c_str());
        if (RedisModule_CallReplyType(set_reply) != REDISMODULE_REPLY_STRING) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to add tracked key." );
            return REDISMODULE_ERR;
        }
    }

    // Save the Query for Tracking
    if(argv[2] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to add query because query is NULL." );
        return REDISMODULE_ERR;
    }
    std::string query_str = RedisModule_StringPtrLen(argv[2], NULL);
    std::string query_term = Get_Query_Term(query_str);
    std::string query_attribute = Get_Query_Attribute(query_str);
    std::string query_tracking_key_str = CCT_MODULE_QUERY_PREFIX + client_name_str + CCT_MODULE_KEY_SEPERATOR + query_term + CCT_MODULE_KEY_SEPERATOR + query_attribute;

    RedisModuleKey *query_tracking_key = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, query_tracking_key_str.c_str() , query_tracking_key_str.length()) , REDISMODULE_WRITE );
    if (RedisModule_StringSet(query_tracking_key, RedisModule_CreateString(ctx, "" , 1) ) != REDISMODULE_OK ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed while registering query tracking key: " +  query_tracking_key_str);
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    if (RedisModule_Init(ctx,"CCT",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    
    if (RedisModule_CreateCommand(ctx,"CCT.REGISTER", Register_RedisCommand , "admin write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT.REGISTER command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT.FT.SEARCH", FT_Search_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT.FT.SEARCH command created successfully.");
    }
    
    return REDISMODULE_OK;
}


#ifdef __cplusplus
}
#endif

