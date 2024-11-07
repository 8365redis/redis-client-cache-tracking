#pragma once

#include "redismodule.h"

void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_name, const std::vector<std::string> &key_ids, const std::string index, bool is_wildcard = false);
void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client);
void Add_Tracking_Key_Old_Value(RedisModuleCtx *ctx, std::string key, bool delete_old);
void Update_Tracking_Query(RedisModuleCtx *ctx, const std::string query_str, const std::string new_key);
int Add_Event_To_Stream(RedisModuleCtx *ctx, const std::string client, const std::string event, const std::string key, const std::string value, const std::string queries, bool send_old_value = false);
int Trim_Stream_By_ID(RedisModuleCtx *ctx, RedisModuleString *last_read_id, std::string client_name);
void Handle_Deleted_Key(RedisModuleCtx *ctx, const std::string deleted_key);
