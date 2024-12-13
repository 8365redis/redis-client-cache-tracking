#pragma once

#include <string>
#include "redismodule.h"

int Subscribe_Index_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Setup_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Index_Subscription_Handler(RedisModuleCtx *ctx);
void Index_Subscription_Handler(RedisModuleCtx *ctx);
void Process_Index_Subscription(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size);
