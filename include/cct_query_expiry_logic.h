#pragma once

#include "redismodule.h"

int Handle_Query_Expire(RedisModuleCtx *ctx , std::string key);
