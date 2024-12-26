#pragma once

#include "redismodule.h"

void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_name, const std::vector<std::string> &key_ids, const std::string index, bool is_wildcard = false);
void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client, bool renew = false);
void Add_Tracking_Key_Old_Value(RedisModuleCtx *ctx, std::string key, bool delete_old);
void Update_Tracking_Query(RedisModuleCtx *ctx, const std::string query_str, const std::string new_key);
int Add_Event_To_Stream(RedisModuleCtx *ctx, const std::string stream_name, const std::string event, const std::string key, const std::string value, const std::string queries, bool send_old_value = false, bool index_subscription = false, bool snapshot_buffer = false);
int Trim_Stream_By_ID(RedisModuleCtx *ctx, RedisModuleString *last_read_id, std::string client_name);
void Handle_Deleted_Key(RedisModuleCtx *ctx, const std::string deleted_key);
void Renew_Queries(RedisModuleCtx *ctx, std::vector<std::string> queries, const std::string client_tracking_group, unsigned long long client_ttl);
std::string Get_Key_Queries(RedisModuleCtx *ctx, const std::string key);
