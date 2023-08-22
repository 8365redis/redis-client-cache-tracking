#include "redismodule.h"
#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>
#include <sstream>
#include <errno.h>
#include <vector>
#include <iterator>
#include <algorithm> 
#include <chrono>
#include <iomanip>


#define MAX_KEY_SIZE 1000
#define CCT_MODULE_PREFIX "CCT:"
#define CCT_MODULE_CLIENT_PREFIX "CCT:CLIENT:"
#define LOG(ctx, level, log) Log_Std_Output(ctx, level, log)

#ifdef __cplusplus
extern "C" {
#endif

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch() % std::chrono::seconds{1});

    auto time = std::put_time(&tm, "%d %b %Y %H:%M:%S");
    std::cout<<"XXXXX:X "<<time<<"."<<std::to_string(ms.count())<<" * <CCT_MODULE> "<< fmt << std::endl;
}

void Log_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    std::stringstream argument_stream;
    std::string command_name = RedisModule_StringPtrLen(argv[0], NULL);
    for ( int i = 1; i < argc; i++) {
        argument_stream<<RedisModule_StringPtrLen(argv[i], NULL)<< " ";
    }
    Log_Std_Output(ctx, REDISMODULE_LOGLEVEL_DEBUG , command_name + " command called with arguments " + argument_stream.str());
}

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    //RedisModule_AutoMemory(ctx);

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
        return REDISMODULE_OK;
    } else if (RedisModule_CallReplyType(get_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "NotifyCallback event : " + event_str  + " , key " + key_str + " getting the stream key failed.");
        return REDISMODULE_ERR;
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " is tracked.");
    RedisModuleString *stream_name = RedisModule_CreateStringFromCallReply(get_reply);
    
    RedisModuleString *value;
    RedisModuleCallReply *json_get_reply = RedisModule_Call(ctx, "JSON.GET", "s", key);
    if (RedisModule_CallReplyType(json_get_reply) == REDISMODULE_REPLY_NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "NotifyCallback event : " + event_str  + " , key " + key_str + " getting the value which is gone.");
        value = RedisModule_CreateString( ctx, "" , 0 );
    } else if (RedisModule_CallReplyType(json_get_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "NotifyCallback event : " + event_str  + " , key " + key_str + " getting the value failed.");
        return REDISMODULE_ERR;
    } else {
        value = RedisModule_CreateStringFromCallReply(json_get_reply);
    }

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
    
    if (RedisModule_SetClientNameById(client_id, argv[1]) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    RedisModuleCallReply *exists_reply = RedisModule_Call(ctx, "EXISTS", "s", argv[1]);
    if (RedisModule_CallReplyType(exists_reply) != REDISMODULE_REPLY_INTEGER) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to detect the stream." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    int stream_count = RedisModule_CallReplyInteger(exists_reply);

    if ( stream_count > 0 ){
        RedisModuleCallReply *del_reply = RedisModule_Call(ctx, "DEL", "s", argv[1]);
        if (RedisModule_CallReplyType(del_reply) != REDISMODULE_REPLY_INTEGER) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to delete the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }
    
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
    // Startin from 1 as first one count
    for (size_t i = 1; i < reply_length; i++) {
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
        else
        {
            std::cout<<"Null"<<std::endl;
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
    
    for (const auto& it : key_ids) {
        std::string key_with_prefix = CCT_MODULE_PREFIX + it; 
        RedisModuleCallReply *set_reply = RedisModule_Call(ctx, "SET", "cc", key_with_prefix.c_str()  , client_name_str.c_str());
        if (RedisModule_CallReplyType(set_reply) != REDISMODULE_REPLY_STRING) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to add tracked key." );
            return REDISMODULE_ERR;
        }
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

