#ifndef CCT_COMMAND_HEARTBEAT_H
#define CCT_COMMAND_HEARTBEAT_H

#include "redismodule.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* CCT_COMMAND_HEARTBEAT_H */