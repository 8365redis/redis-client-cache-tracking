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

#define MAX_KEY_SIZE 1000
#define CCT_MODULE_PREFIX "CCT:"
#define RedisModule_Log(ctx, level, log) std::cout<<"[CCT_MODULE] "<<log<<std::endl

#ifdef __cplusplus
extern "C" {
#endif

void Log_Command(RedisModuleString **argv, int argc){
    std::stringstream argument_stream;
    std::string command_name = RedisModule_StringPtrLen(argv[0], NULL);
    for ( int i = 1; i < argc; i++) {
        argument_stream<<RedisModule_StringPtrLen(argv[i], NULL)<< " ";
    }
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , command_name + " command called with arguments " + argument_stream.str());
}

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    //RedisModule_AutoMemory(ctx);

    std::string event_str = event;
    std::string key_str = RedisModule_StringPtrLen(key, NULL);
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str);

    // Ignore our self events
    if (key_str.rfind(CCT_MODULE_PREFIX, 0) == 0){
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
        return REDISMODULE_OK;
    }

    // Add prefix
    std::stringstream prefix_stream;
    prefix_stream<<CCT_MODULE_PREFIX<<key_str;
    std::string key_with_prefix = prefix_stream.str(); 

    // Check if the key is tracked
    RedisModuleCallReply *get_reply = RedisModule_Call(ctx, "GET", "c", key_with_prefix.c_str());
    if (RedisModule_CallReplyType(get_reply) == REDISMODULE_REPLY_NULL){
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " is not tracked : " + key_with_prefix);
        return REDISMODULE_OK;
    } else if (RedisModule_CallReplyType(get_reply) != REDISMODULE_REPLY_STRING) {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "NotifyCallback event : " + event_str  + " , key " + key_str + " getting the stream key failed.");
        return REDISMODULE_ERR;
    }
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , "NotifyCallback event : " + event_str  + " , key " + key_str + " is tracked.");
    RedisModuleString *stream_name = RedisModule_CreateStringFromCallReply(get_reply);
    
    RedisModuleString *value;
    RedisModuleCallReply *json_get_reply = RedisModule_Call(ctx, "JSON.GET", "s", key);
    if (RedisModule_CallReplyType(json_get_reply) == REDISMODULE_REPLY_NULL){
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "NotifyCallback event : " + event_str  + " , key " + key_str + " getting the value which is gone.");
        value = RedisModule_CreateString( ctx, "" , 0 );
    } else if (RedisModule_CallReplyType(json_get_reply) != REDISMODULE_REPLY_STRING) {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "NotifyCallback event : " + event_str  + " , key " + key_str + " getting the value failed.");
        return REDISMODULE_ERR;
    } else {
        value = RedisModule_CreateStringFromCallReply(json_get_reply);
    }

    // Write to stream
    
    RedisModuleCallReply *xadd_reply =  RedisModule_Call(ctx, "XADD", "sccs", stream_name , "*", key_str.c_str() , value);
    if (RedisModule_CallReplyType(xadd_reply) != REDISMODULE_REPLY_STRING) {
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    return REDISMODULE_OK;
}

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    RedisModule_AutoMemory(ctx);
    
    if (argc != 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    Log_Command(argv,argc);

    unsigned long long client_id = RedisModule_GetClientId(ctx);
    
    if (RedisModule_SetClientNameById(client_id, argv[1]) != REDISMODULE_OK){
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to set client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    RedisModuleCallReply *exists_reply = RedisModule_Call(ctx, "EXISTS", "s", argv[1]);
    if (RedisModule_CallReplyType(exists_reply) != REDISMODULE_REPLY_INTEGER) {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to detect the stream." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    int stream_count = RedisModule_CallReplyInteger(exists_reply);

    if ( stream_count > 0 ){
        RedisModuleCallReply *del_reply = RedisModule_Call(ctx, "DEL", "s", argv[1]);
        if (RedisModule_CallReplyType(del_reply) != REDISMODULE_REPLY_INTEGER) {
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to delete the stream." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
    }
    
    if ( RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_SET | REDISMODULE_NOTIFY_STRING |
            REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_NEW | REDISMODULE_NOTIFY_MODULE ,
             NotifyCallback) != REDISMODULE_OK ) {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to SubscribeToKeyspaceEvents." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));   
    }

    RedisModule_ReplyWithSimpleString(ctx, "Done");
    return REDISMODULE_OK;
}

int FT_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    RedisModule_AutoMemory(ctx);
    
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    
    Log_Command(argv,argc);
    
    // Forward Search
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.SEARCH", "v", argv + 1, argc - 1, "NOCONTENT");
    
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    size_t reply_length = RedisModule_CallReplyLength(reply);
    RedisModule_ReplyWithArray(ctx , reply_length);
    RedisModule_ReplyWithLongLong(ctx, reply_length-1); // Minus 1 because we don't count the size just the results
    std::vector<std::string> keys;
    // Startin from 1 as first one count
    for (size_t i = 1; i < RedisModule_CallReplyLength(reply); i++) {
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(reply, i);
        RedisModuleString *response = RedisModule_CreateStringFromCallReply(key_reply);
        const char *response_str = RedisModule_StringPtrLen(response, NULL);
        RedisModule_ReplyWithStringBuffer(ctx, response_str, strlen(response_str));
        keys.push_back(RedisModule_StringPtrLen(response, NULL));
    }

    std::stringstream  s;
    copy(keys.begin(),keys .end(), std::ostream_iterator<std::string>(s," "));
    RedisModule_Log( ctx, REDISMODULE_LOGLEVEL_DEBUG , "FT.SEARCH returned : " + s.str());

    unsigned long long client_id = RedisModule_GetClientId(ctx);
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id); 
    if ( client_name == NULL){
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to get client name." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if ( client_name_str.empty()){
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed because client name is not set." );
        return RedisModule_ReplyWithError(ctx, "Client name is not set or set to empty");
    }

    for (const auto& it : keys) {
        std::string key_with_prefix = CCT_MODULE_PREFIX + it; 
        RedisModuleCallReply *set_reply = RedisModule_Call(ctx, "SET", "cc", key_with_prefix.c_str()  , client_name_str.c_str());
        if (RedisModule_CallReplyType(set_reply) != REDISMODULE_REPLY_STRING) {
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "FT_Search_RedisCommand failed to add tracked key." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
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
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT.REGISTER command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT.FT.SEARCH", FT_Search_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT.FT.SEARCH command created successfully.");
    }
    
    return REDISMODULE_OK;
}


#ifdef __cplusplus
}
#endif

