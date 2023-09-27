#include "cct_command_heartbeat.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Log_Command(ctx,argv,argc);
    
    if (argc > 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    if (Is_Client_Connected(Get_Client_Name(ctx)) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed : Client is not registered" );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    // Check if we have to trim the stream
    bool just_update_ttl = false;
    if (argc == 2) {
        RedisModuleString *last_read_id = argv[1];
    } else {
        just_update_ttl = true;
    }

    // Regardless update the TTL
    if ( Update_Client_TTL(ctx) == false ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed.");
        return RedisModule_ReplyWithError(ctx, "Updating the TTL failed");
    }  

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}