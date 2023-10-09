#ifndef CCT_COMMAND_REGISTER_H
#define CCT_COMMAND_REGISTER_H

#include <errno.h>
#include <string.h>
#include <vector>
#include <string>
#include <unordered_map>

#include "redismodule.h"
#include "logger.h"
#include "constants.h"
#include "cct_query_tracking_data.h"
#include "json_handler.h"
#include "client_tracker.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Send_Snapshot(RedisModuleCtx *ctx, RedisModuleKey *stream_key, std::string client);

#endif /* CCT_COMMAND_REGISTER_H */