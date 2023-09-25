#ifndef CCT_QUERY_TRACKING_DATA_H
#define CCT_QUERY_TRACKING_DATA_H

#include <errno.h>
#include <string.h>
#include <vector>

#include "redismodule.h"
#include "logger.h"
#include "constants.h"
#include "query_parser.h"

void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_name, const std::vector<std::string> &key_ids);
void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client);
int Add_Event_To_Stream(RedisModuleCtx *ctx, const std::string client, const std::string event, const std::string key, const std::string value, const std::string queries);


#endif /* CCT_QUERY_TRACKING_DATA_H */