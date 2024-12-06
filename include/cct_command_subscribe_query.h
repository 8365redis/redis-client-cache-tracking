#pragma once

#include "redismodule.h"

int Subscribe_Query_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
