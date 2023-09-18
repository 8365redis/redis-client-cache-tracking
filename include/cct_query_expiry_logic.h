#ifndef CCT_QUERY_EXPIRY_LOGIC_H
#define CCT_QUERY_EXPIRY_LOGIC_H

#include "redismodule.h"
#include "logger.h"
#include "constants.h"
#include "cct_query_tracking_logic.h"

int Handle_Query_Expire(RedisModuleCtx *ctx , std::string key);

#endif /* CCT_QUERY_EXPIRY_LOGIC_H */