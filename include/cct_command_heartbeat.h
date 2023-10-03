#ifndef CCT_COMMAND_HEARTBEAT_H
#define CCT_COMMAND_HEARTBEAT_H

#include <errno.h>
#include <string.h>
#include <vector>
#include <string>
#include <unordered_map>

#include "redismodule.h"
#include "logger.h"
#include "constants.h"
#include "client_tracker.h"
#include "cct_query_tracking_data.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* CCT_COMMAND_HEARTBEAT_H */