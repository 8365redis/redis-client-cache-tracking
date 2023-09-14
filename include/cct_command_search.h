#ifndef CCT_COMMAND_SEARCH_H
#define CCT_COMMAND_SEARCH_H


#include <errno.h>
#include <string.h>
#include <vector>
#include <string>

#include "redismodule.h"
#include "logger.h"
#include "cct_query_tracking_data.h"
#include "client_tracker.h"

int FT_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* CCT_COMMAND_SEARCH_H */