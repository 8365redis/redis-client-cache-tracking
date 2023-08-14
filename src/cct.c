#include "redismodule.h"
#include <stdlib.h>

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2 ) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModuleString *app_name = argv[1];
    //RedisModuleCallReply *reply = RedisModule_Call(ctx, "SET", "sc", app_name, "10");

    RedisModule_ReplyWithSimpleString(ctx, "Register successful");
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"CCT",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,"CCT.REGISTER", Register_RedisCommand, "", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

