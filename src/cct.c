#include "redismodule.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    unsigned long long client_id = RedisModule_GetClientId(ctx);
    
    if (RedisModule_SetClientNameById(client_id, argv[1]) != REDISMODULE_OK){
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    if ( RedisModule_Call(ctx, "EXISTS", "s", argv[1]) > 0 ){
        RedisModule_Call(ctx, "DEL", "s", argv[1]);
    }
    
    RedisModule_Call(ctx, "XADD", "sccc", argv[1], "*", "key", "val");

    //RedisModuleCallReply *reply = RedisModule_Call(ctx, "SET", "sl", argv[1], client_id);
   
    RedisModule_ReplyWithSimpleString(ctx, "Done");
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"CCT",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,"CCT.REGISTER", Register_RedisCommand , "admin write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

