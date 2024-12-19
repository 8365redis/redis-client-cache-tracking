#pragma once

#include <string>
#include "redismodule.h"

int Subscribe_Index_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Enable_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Index_Subscription_Handler(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size);
void Process_Index_Subscription(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size);
int Disable_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Repopulate_Index_Stream(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void Set_Index_In_Setup(RedisModuleCtx *ctx, std::string index_name, bool value);
bool Is_Index_In_Setup(std::string index_name);
void Queue_Key_Space_Notification(std::string index_name, std::string event, std::string key);
void Process_Queued_Key_Space_Notifications(RedisModuleCtx *ctx, std::string index_name);