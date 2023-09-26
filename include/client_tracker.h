#ifndef CLIENT_TRACKER_H
#define CLIENT_TRACKER_H

#include "redismodule.h"
#include <unordered_map>
#include <string>

void handleClientEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);
void Connect_Client(std::string client);
void Disconnect_Client(std::string client);
bool Client_Connected(std::string client);

#endif /* CLIENT_TRACKER_H */