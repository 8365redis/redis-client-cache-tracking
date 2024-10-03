#ifndef CCT_QUERY_TRACKING_LOGIC_H
#define CCT_QUERY_TRACKING_LOGIC_H

#include <vector>
#include <string>
#include <set>
#include <set>
#include <unordered_map>
#include "redismodule.h"

int Get_Tracking_Clients_From_Changed_JSON(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key,
                                             std::vector<std::string> &clients_to_update, std::string &json_str, 
                                             std::unordered_map<std::string, std::vector<std::string>> &client_to_queries_map,
                                             std::set<std::string> &current_queries);

int Query_Track_Check(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key, std::vector<std::string> already_tracking_clients);
int Notify_Callback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key);

#endif /* CCT_QUERY_TRACKING_LOGIC_H */