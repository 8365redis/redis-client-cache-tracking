#ifndef CCT_COMMAND_REGISTER_H
#define CCT_COMMAND_REGISTER_H

#include <errno.h>
#include <string.h>

#include "redismodule.h"
#include "logger.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* CCT_COMMAND_REGISTER_H */