#include "redismodule.h"
#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>
#include <sstream>
#include <errno.h>
#include <vector>
#include <iterator>


#define MAX_KEY_SIZE 1000
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

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    Log_Command(argv,argc);

    unsigned long long client_id = RedisModule_GetClientId(ctx);
    
    if (RedisModule_SetClientNameById(client_id, argv[1]) != REDISMODULE_OK){
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
    
    RedisModule_Call(ctx, "XADD", "sccc", argv[1], "*", "key", "val");

    //RedisModuleCallReply *reply = RedisModule_Call(ctx, "SET", "sl", argv[1], client_id);

    RedisModuleCallReply *tracking_reply = RedisModule_Call(ctx, "CLIENT", "cc", "TRACKING", "ON");
    if (RedisModule_CallReplyType(tracking_reply) != REDISMODULE_REPLY_STRING) {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to enable tracking." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }    
   
    RedisModule_ReplyWithSimpleString(ctx, "Done");
    return REDISMODULE_OK;
}

int FT_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);
    
    Log_Command(argv,argc);
    
    // Call FT.SEARCH
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.SEARCH", "v", argv + 1, argc - 1);
    
    // Check reply type
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

    if (RedisModule_CreateCommand(ctx,"CCT.FT_SEARCH", FT_Search_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT.FT_SEARCH command created successfully.");
    }
    
    return REDISMODULE_OK;
}


#ifdef __cplusplus
}
#endif

