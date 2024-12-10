#pragma once

#include <string>
#include "redismodule.h"

const long long DEFAULT_CHUNK_SIZE = 2;

int Subscribe_Query_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Query_Subscription(RedisModuleCtx *ctx, std::string index_name, std::string query_str, int chunk_size);
void Process_Query_Subscription(RedisModuleCtx *ctx, std::string index_name, std::string query_str, int chunk_size);