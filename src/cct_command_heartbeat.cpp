#include "cct_command_heartbeat.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Log_Command(ctx,argv,argc);
    
    if (argc > 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    std::string client_name = Get_Client_Name(ctx);

    if (Is_Client_Connected(client_name) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed : Client is not registered" );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    // Regardless update the TTL
    if ( Update_Client_TTL(ctx) == false ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed.");
        return RedisModule_ReplyWithError(ctx, "Updating the TTL failed");
    }

    // Check if we have to trim the stream
    if (argc == 2) {
        if ( Trim_From_Stream(ctx, argv[1], client_name) == REDISMODULE_ERR ){
            return RedisModule_ReplyWithError(ctx, "Trim with given Stream ID failed");
        }
    } 
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}