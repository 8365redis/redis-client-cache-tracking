#ifndef CCT_COMMAND_AGGREGATE_H
#define CCT_COMMAND_AGGREGATE_H

#include "redismodule.h"

int Aggregate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Aggregate_Handler(RedisModuleCtx *ctx);

#endif /* CCT_COMMAND_AGGREGATE_H */