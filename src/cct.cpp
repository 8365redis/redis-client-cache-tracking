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
#include <regex>
#include <set>


#ifdef __cplusplus
extern "C" {
#endif

static std::set<unsigned long long> CCT_REGISTERED_CLIENTS;

std::string Get_Client_Name(RedisModuleCtx *ctx){

    unsigned long long client_id = RedisModule_GetClientId(ctx);
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id); 
    if ( client_name == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Failed to get client name." );
        return "";
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if ( client_name_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Failed to get client name because client name is not set." );
        return "";
    }
    return client_name_str;
}

int Get_Traking_Clients_From_Changed_JSON(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key, std::vector<std::string> &clients_to_update, std::string &json_str ) {
    RedisModule_AutoMemory(ctx);

    std::string key_str = RedisModule_StringPtrLen(r_key, NULL);
    
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Traking_Clients_From_Changed_JSON :  key " + key_str + " for event: " + event);

    RedisModuleString *value = Get_JSON_Value(ctx, "" , r_key);
    if (value == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Traking_Clients_From_Changed_JSON failed while getting JSON value for key: " +  key_str);
        return REDISMODULE_ERR;
    }

    json_str = RedisModule_StringPtrLen(value, NULL);
    if ( json_str.empty() ){ 
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Traking_Clients_From_Changed_JSON :  JSON value is empty for key: " + key_str);
        return REDISMODULE_OK;
    }

    std::vector<std::string> queries;
    Recursive_JSON_Iterate(Get_JSON_Object(json_str) , "", queries);

    for (auto & q : queries) {
        std::string query_with_prefix = CCT_MODULE_QUERY_PREFIX + q;
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Traking_Clients_From_Changed_JSON check this query for tracking: " + query_with_prefix);
        RedisModuleCallReply *smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", query_with_prefix.c_str());
        if (RedisModule_CallReplyType(smembers_reply) != REDISMODULE_REPLY_ARRAY ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Traking_Clients_From_Changed_JSON failed while getting client names for query: " +  query_with_prefix);
            return REDISMODULE_ERR;
        } else {
            const size_t reply_length = RedisModule_CallReplyLength(smembers_reply);
            for (size_t i = 0; i < reply_length; i++) {
                RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(smembers_reply, i);
                if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
                    RedisModuleString *client = RedisModule_CreateStringFromCallReply(key_reply);
                    const char *client_str = RedisModule_StringPtrLen(client, NULL);
                    clients_to_update.push_back(std::string(client_str));
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Traking_Clients_From_Changed_JSON query matched to this client(app): " + (std::string)client_str);
                    
                    std::string key_with_prefix = CCT_MODULE_TRACKING_PREFIX + key_str;
                    RedisModuleCallReply *sadd_key_reply = RedisModule_Call(ctx, "SADD", "cc", key_with_prefix.c_str()  , client_str);
                    if (RedisModule_CallReplyType(sadd_key_reply) != REDISMODULE_REPLY_INTEGER ){
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Traking_Clients_From_Changed_JSON failed while registering tracking key: " +  key_with_prefix);
                        return REDISMODULE_ERR;
                    } else {
                        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Traking_Clients_From_Changed_JSON added to stream: " + key_with_prefix);
                    }
                }
            }
        }
    }

    return REDISMODULE_OK;
}

int Query_Track_Check(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key, std::vector<std::string> already_tracking_clients){
    RedisModule_AutoMemory(ctx);

    std::vector<std::string> clients_to_update;
    std::string json_str;
    Get_Traking_Clients_From_Changed_JSON(ctx , event,  r_key , clients_to_update , json_str);

    std::string key_str = RedisModule_StringPtrLen(r_key, NULL);

    std::set<std::string> tracking_clients_set(already_tracking_clients.begin(), already_tracking_clients.end());
    std::set<std::string> clients_to_update_set(clients_to_update.begin(), clients_to_update.end());

    std::set<std::string> total_clients;
    std::set_union(std::begin(tracking_clients_set), std::end(tracking_clients_set), std::begin(clients_to_update_set), std::end(clients_to_update_set), std::inserter(total_clients, std::begin(total_clients)));    
    // Write to stream
    for (auto & client_name : total_clients) {
        RedisModuleCallReply *xadd_reply =  RedisModule_Call(ctx, "XADD", "ccsc", client_name.c_str() , "*", r_key , json_str.c_str());
        if (RedisModule_CallReplyType(xadd_reply) != REDISMODULE_REPLY_STRING) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed to create the stream." );
                return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }
    // Now delete the tracked keys which are not matching to our queries anymore
    std::set<std::string> diff_clients;
    // tracking_clients_set - clients_to_update_set
    std::set_difference (tracking_clients_set.begin(), tracking_clients_set.end(), clients_to_update_set.begin(), clients_to_update_set.end(), inserter(diff_clients, diff_clients.begin()));
    // Delete no more tracked keys
    for (const auto& it : diff_clients) {
        std::string key_with_prefix = CCT_MODULE_TRACKING_PREFIX + key_str;
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check will delete no more interested client: " + it + " from tracked key : " +  key_with_prefix);
        RedisModuleCallReply *sadd_key_reply = RedisModule_Call(ctx, "SREM", "cc", key_with_prefix.c_str()  , it.c_str());
        if (RedisModule_CallReplyType(sadd_key_reply) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting tracking key: " +  key_with_prefix);
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    RedisModule_AutoMemory(ctx);

    std::string event_str = event;
    std::string key_str = RedisModule_StringPtrLen(key, NULL);

    if (strcasecmp(event, "loaded") == 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " : Loaded is IGNORED");
        return REDISMODULE_OK;
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str);

    // Ignore our self events
    if (key_str.rfind(CCT_MODULE_PREFIX, 0) == 0){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
        return REDISMODULE_OK;
    }

    // Add prefix
    std::stringstream prefix_stream;
    prefix_stream<<CCT_MODULE_TRACKING_PREFIX<<key_str;
    std::string key_with_prefix = prefix_stream.str();


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


    // Check if the new set is matching a new 
    if ( strcasecmp(event, "json.set") == 0  || strcasecmp(event, "del") == 0 ) {
        Query_Track_Check(ctx, event_str, key, already_tracking_clients);
        return REDISMODULE_OK;
    }
    
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " is tracked.");

 
    //Write to stream
    for (auto client : already_tracking_clients){
        RedisModuleString *value = Get_JSON_Value(ctx, event_str , key);
        RedisModuleCallReply *xadd_reply =  RedisModule_Call(ctx, "XADD", "cccs", client.c_str() , "*", key_str.c_str() , value);
        if (RedisModule_CallReplyType(xadd_reply) != REDISMODULE_REPLY_STRING) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
                return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
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
    if(CCT_REGISTERED_CLIENTS.count(client_id) != 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand ignored with duplicate call. Client ID : " +  std::to_string(client_id));
        return RedisModule_ReplyWithError(ctx, "Duplicate Register");
    }
    CCT_REGISTERED_CLIENTS.insert(client_id);
    
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

    std::string client_name_str = Get_Client_Name(ctx);
    if ( client_name_str.empty()){
        return REDISMODULE_ERR;
    }
    
    // Add tracked keys
    for (const auto& it : key_ids) {
        std::string key_with_prefix = CCT_MODULE_TRACKING_PREFIX + it;
        std::cout<<"Tracked Keys:"<<key_with_prefix<<std::endl;
        
        RedisModuleCallReply *sadd_key_reply = RedisModule_Call(ctx, "SADD", "cc", key_with_prefix.c_str()  , client_name_str.c_str());
        if (RedisModule_CallReplyType(sadd_key_reply) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed while registering query tracking key: " +  key_with_prefix);
            return REDISMODULE_ERR;
        }
        
    }

    // Save the Query for Tracking
    if(argv[2] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to add query because query is NULL.");
        return REDISMODULE_ERR;
    }
    std::string query_str = RedisModule_StringPtrLen(argv[2], NULL);
    std::string query_term = Get_Query_Term(query_str);
    std::string query_attribute = Get_Query_Attribute(query_str);
    std::string query_tracking_key_str = CCT_MODULE_QUERY_PREFIX + query_term + CCT_MODULE_KEY_SEPERATOR + query_attribute ;
    RedisModuleString *client_name = RedisModule_CreateString(ctx, client_name_str.c_str() , client_name_str.length());
    
    RedisModuleCallReply *sadd_reply = RedisModule_Call(ctx, "SADD", "cs", query_tracking_key_str.c_str()  , client_name);
    if (RedisModule_CallReplyType(sadd_reply) != REDISMODULE_REPLY_INTEGER ){
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

    // Subscribe to key space events
    if ( RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_SET | REDISMODULE_NOTIFY_STRING |
            REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_NEW | REDISMODULE_NOTIFY_MODULE ,
             NotifyCallback) != REDISMODULE_OK ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to SubscribeToKeyspaceEvents." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));   
    }
    
    return REDISMODULE_OK;
}


#ifdef __cplusplus
}
#endif

