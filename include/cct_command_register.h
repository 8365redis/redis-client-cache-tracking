#pragma once

#include "redismodule.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Send_Snapshot(RedisModuleCtx *ctx, RedisModuleKey *stream_key, std::string client);

