#pragma once

#include "redismodule.h"
#include <string>

int Handle_Query_Expire(RedisModuleCtx *ctx , std::string key);
int Clean_Up_Query(RedisModuleCtx *ctx , std::string client_name, std::string index_and_query, bool delete_qc_key = false);
