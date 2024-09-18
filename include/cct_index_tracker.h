#ifndef CCT_INDEX_TRACKER_H
#define CCT_INDEX_TRACKER_H

#include <set>
#include <string>
#include "redismodule.h"

void OnRedisReady(RedisModuleCtx *ctx, RedisModuleEvent event, uint64_t subevent, void *data);
std::set<std::string> Get_All_Indexes(RedisModuleCtx *ctx);
void Get_Index_Prefixes(RedisModuleCtx *ctx, std::set<std::string> indexes);
std::string Get_Tracked_Index_From_Key(std::string key);
const std::set<std::string> Get_Tracked_Index_Clients(std::string index);
void Track_Index(std::string index, std::string client_name);
void UnTrack_Index(std::string index, std::string client_name);

#endif /* CCT_INDEX_TRACKER_H */