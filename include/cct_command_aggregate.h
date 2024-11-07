#pragma once

#include "redismodule.h"

int Aggregate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Aggregate_Handler(RedisModuleCtx *ctx);
int Invalidate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

