#pragma once

#include "redismodule.h"

void Handle_RDB_Event(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
int Handle_Offline_Query_Expire(RedisModuleCtx *ctx);
