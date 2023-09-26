#include "cct_command_heartbeat.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Log_Command(ctx,argv,argc);
    
    if (argc > 2 ) {
        return RedisModule_WrongArity(ctx);
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
        return REDISMODULE_ERR;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}