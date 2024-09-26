#ifndef CCT_QUERY_TRACKING_DATA_H
#define CCT_QUERY_TRACKING_DATA_H

#include <errno.h>
#include <string.h>
#include <vector>

#include "redismodule.h"
#include "logger.h"
#include "constants.h"
#include "query_parser.h"
#include "client_tracker.h"
#include "json_handler.h"

void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_name, const std::vector<std::string> &key_ids);
void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client);
void Add_Tracking_Key_Old_Value(RedisModuleCtx *ctx, std::string key, bool delete_old);
void Update_Tracking_Query(RedisModuleCtx *ctx, const std::string query_str, const std::string new_key);
int Add_Event_To_Stream(RedisModuleCtx *ctx, const std::string client, const std::string event, const std::string key, const std::string value, const std::string queries, bool send_old_value = false);
int Trim_From_Stream(RedisModuleCtx *ctx, RedisModuleString *last_read_id, std::string client_name);
void Add_Tracking_Wildcard_Query(RedisModuleCtx *ctx, std::string query_str, std::string client_tracking_group);

#endif /* CCT_QUERY_TRACKING_DATA_H */