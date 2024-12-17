#pragma once

#include <string>
#include "redismodule.h"

int Subscribe_Index_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Setup_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Index_Subscription_Handler(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size);
void Process_Index_Subscription(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size);
int Disable_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
std::string Get_Index_Latest_Stream_Name(std::string index_name);