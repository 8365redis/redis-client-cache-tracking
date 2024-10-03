#ifndef CCT_COMMAND_SEARCH_H
#define CCT_COMMAND_SEARCH_H

#include "redismodule.h"

int FT_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* CCT_COMMAND_SEARCH_H */