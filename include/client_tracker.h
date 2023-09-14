#ifndef CLIENT_TRACKER_H
#define CLIENT_TRACKER_H

#include "redismodule.h"
#include <unordered_map>
#include <string>

extern std::unordered_map<std::string, unsigned long long> CCT_REGISTERED_CLIENTS;

void handleClientEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);

#endif /* CLIENT_TRACKER_H */