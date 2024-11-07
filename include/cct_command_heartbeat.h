#pragma once

#include "redismodule.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

