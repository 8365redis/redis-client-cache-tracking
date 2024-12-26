#pragma once

#include "redismodule.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Send_Snapshot(RedisModuleCtx *ctx, std::string client);
void Start_Snapshot_Handler(RedisModuleCtx *ctx);
void Snapshot_Handler(RedisModuleCtx *ctx);
bool Is_Snapshot_InProgress(std::string client_name_str);
void Set_Snapshot_InProgress(std::string client_name_str, bool value);
void Add_Snapshot_Event(std::string client_name_str, const std::string event, const std::string key, const std::string value, const std::string queries, bool send_old_value);
void Process_Snapshot_Events(RedisModuleCtx *ctx, std::string client_name_str);
