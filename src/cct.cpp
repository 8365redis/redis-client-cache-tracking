#include "redismodule.h"
#include "constants.h"
#include "query_parser.h"
#include "json_handler.h"
#include "client_tracker.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <iterator>
#include <algorithm>
#include <regex>
#include <set>
#include <unordered_map>

#ifdef __cplusplus
extern "C" {
#endif

void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_name) {
    std::string query_str = RedisModule_StringPtrLen(query, NULL);
    std::string query_term = Get_Query_Term(query_str);
    std::string query_attribute = Get_Query_Attribute(query_str);
    std::string query_tracking_key_str = CCT_MODULE_QUERY_PREFIX + query_term + CCT_MODULE_KEY_SEPERATOR + query_attribute ;
    
    RedisModuleCallReply *sadd_reply = RedisModule_Call(ctx, "SADD", "cc", query_tracking_key_str.c_str()  , client_name.c_str());
    if (RedisModule_CallReplyType(sadd_reply) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed while registering query tracking key: " +  query_tracking_key_str);
    }

    // Save the Query:Client for Tracking and Expire
    std::string query_client_key_name_str = CCT_MODULE_CLIENT_QUERY_PREFIX + query_term + CCT_MODULE_KEY_SEPERATOR + query_attribute + CCT_MODULE_KEY_SEPERATOR + client_name;
    RedisModuleString *query_client_key_name = RedisModule_CreateString(ctx, query_client_key_name_str.c_str() , query_client_key_name_str.length());
    RedisModuleString *empty = RedisModule_CreateString(ctx, "1" , 1);
    RedisModuleKey *query_client_key = RedisModule_OpenKey(ctx, query_client_key_name, REDISMODULE_WRITE);
    if(RedisModule_StringSet(query_client_key, empty) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed while registering client query tracking key: " +  query_client_key_name_str);
    }
    if(RedisModule_SetExpire(query_client_key, CCT_TTL) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed set expire for client query tracking key: " +  query_client_key_name_str);
    }
}

void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client) {
    std::string key_with_prefix = CCT_MODULE_TRACKING_PREFIX + key;
    RedisModuleCallReply *sadd_key_reply = RedisModule_Call(ctx, "SADD", "cc", key_with_prefix.c_str()  , client.c_str());
    if (RedisModule_CallReplyType(sadd_key_reply) != REDISMODULE_REPLY_INTEGER ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Tracking_Clients_From_Changed_JSON failed while registering tracking key: " +  key_with_prefix);
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON added to stream: " + key_with_prefix);
    }
    
    RedisModuleString *key_tracking_set_key_str = RedisModule_CreateString(ctx, key_with_prefix.c_str() , key_with_prefix.length());
    RedisModuleKey *key_tracking_set_key = RedisModule_OpenKey(ctx, key_tracking_set_key_str, REDISMODULE_WRITE);
    if(RedisModule_KeyType(key_tracking_set_key) == REDISMODULE_KEYTYPE_EMPTY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed set expire for client tracking set (key null): " +  key_with_prefix);
    }
    if(RedisModule_SetExpire(key_tracking_set_key, CCT_TTL) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed set expire for client tracking set: " +  key_with_prefix);
    }
}

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
        std::string query_with_prefix = CCT_MODULE_QUERY_PREFIX + q;
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

int Query_Track_Check(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key, std::vector<std::string> already_tracking_clients){
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
        std::ostringstream imploded;
        std::copy(client_queries.begin(), client_queries.end(), std::ostream_iterator<std::string>(imploded, " "));
        client_queries_str = imploded.str();

        RedisModuleCallReply *xadd_reply =  RedisModule_Call(ctx, "XADD", "cccccscccc", client_name.c_str() , "*", "operation" , event.c_str() , 
                                                                "key" , r_key , "value", json_str.c_str(), "queries" , client_queries_str.c_str() );
        if (RedisModule_CallReplyType(xadd_reply) != REDISMODULE_REPLY_STRING) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed to create the stream." );
                return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }
    // Now delete the tracked keys which are not matching to our queries anymore
    std::set<std::string> diff_clients;
    // already_tracking_clients_set - clients_to_update_set
    std::set_difference (already_tracking_clients_set.begin(), already_tracking_clients_set.end(), clients_to_update_set.begin(), clients_to_update_set.end(), inserter(diff_clients, diff_clients.begin()));
    // Delete no more tracked keys
    for (const auto& it : diff_clients) {
        std::string key_with_prefix = CCT_MODULE_TRACKING_PREFIX + key_str;
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check will delete no more interested client: " + it + " from tracked key : " +  key_with_prefix);
        RedisModuleCallReply *srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", key_with_prefix.c_str()  , it.c_str());
        if (RedisModule_CallReplyType(srem_key_reply) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting tracking key: " +  key_with_prefix);
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

int Handle_Expire_Query(RedisModuleCtx *ctx , std::string key) {
    RedisModule_AutoMemory(ctx);
    
    if (key.rfind(CCT_MODULE_KEY_SEPERATOR) == std::string::npos) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Expire_Query key : " + key  + " is invalid.");
        return REDISMODULE_ERR;
    }
    std::string client_name(key.substr(key.rfind(CCT_MODULE_KEY_SEPERATOR) + 1));
    std::string query(key.substr(0, key.rfind(CCT_MODULE_KEY_SEPERATOR)));
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Expire_Query parsed  client_name :" + client_name  + " , query :" + query);
    // FIX HERE : what():  std::out_of_range -> basic_string::substr: __pos (which is 27) > this->size() (which is 22)
    // Handle_Expire_Query parsed  client_name :1 , query :CCT:TRACKED_KEYS:users
    std::string new_query(query.substr(CCT_MODULE_CLIENT_QUERY_PREFIX.length()));
    new_query = CCT_MODULE_QUERY_PREFIX  + new_query;
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Expire_Query parsed  client_name :" + client_name  + " , new_query :" + new_query);

    RedisModuleCallReply *srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", new_query.c_str()  , client_name.c_str());
    if (RedisModule_CallReplyType(srem_key_reply) != REDISMODULE_REPLY_INTEGER){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting client: " +  client_name);
        return REDISMODULE_ERR;
    } else if ( RedisModule_CallReplyInteger(srem_key_reply) == 0 ) { 
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting client (non existing key): " +  client_name);
        return REDISMODULE_ERR;
    }
    
    return REDISMODULE_OK;
}

int Notify_Callback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    RedisModule_AutoMemory(ctx);

    std::string event_str = event;
    std::string key_str = RedisModule_StringPtrLen(key, NULL);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str);

    if( strcasecmp(event, "expired") != 0 && strcasecmp(event, "json.set") != 0  && strcasecmp(event, "del") != 0 )
    {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " not interested event." );
        return REDISMODULE_OK;        
    }

    // Ignore our self events
    if (key_str.rfind(CCT_MODULE_PREFIX, 0) == 0 && strcasecmp(event, "expired") != 0){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
        return REDISMODULE_OK;
    }
    
    if (key_str.rfind(CCT_MODULE_TRACKING_PREFIX, 0) == 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
        return REDISMODULE_OK;        
    }

    if (strcasecmp(event, "expired") == 0 ) {
        return Handle_Expire_Query(ctx, key_str);
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

    Query_Track_Check(ctx, event_str, key, already_tracking_clients);
    
    return REDISMODULE_OK;
}

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Log_Command(ctx,argv,argc);
    
    if (argc != 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *client_name = argv[1];
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    
    // Set client name
    if (RedisModule_SetClientNameById(client_id, client_name) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    // Check if the stream exists and delete if it
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
    RedisModule_StreamTrimByLength(stream_key, 0, 0);  
 
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
        Add_Tracking_Key(ctx, it, client_name_str);          
    }

    // Save the Query for Tracking
    if(argv[2] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to add query because query is NULL.");
        return REDISMODULE_ERR;
    }

    Add_Tracking_Query(ctx, argv[2], client_name_str);

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
             Notify_Callback) != REDISMODULE_OK ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "RedisModule_OnLoad failed to SubscribeToKeyspaceEvents." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ClientChange,
                                             handleClientEvent) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "RedisModule_OnLoad failed to SubscribeToServerEvent." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
  
    return REDISMODULE_OK;
}


#ifdef __cplusplus
}
#endif

