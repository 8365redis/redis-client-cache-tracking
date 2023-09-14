#ifndef CCT_QUERY_TRACKING_LOGIC_H
#define CCT_QUERY_TRACKING_LOGIC_H

#include <errno.h>
#include <string.h>
#include <vector>
#include <string>
#include <set>

#include "redismodule.h"
#include "logger.h"
#include "constants.h"
#include "json_handler.h"
#include "cct_query_tracking_data.h"

int Get_Tracking_Clients_From_Changed_JSON(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key,
                                             std::vector<std::string> &clients_to_update, std::string &json_str, 
                                             std::unordered_map<std::string, std::vector<std::string>> &client_to_queries_map );

int Query_Track_Check(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key, std::vector<std::string> already_tracking_clients);
int Handle_Query_Expire(RedisModuleCtx *ctx , std::string key);
int Notify_Callback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key);

#endif /* CCT_QUERY_TRACKING_LOGIC_H */