#ifndef CLIENT_TRACKER_H
#define CLIENT_TRACKER_H

#include "redismodule.h"
#include <unordered_map>
#include <string>
#include <vector>
#include <thread>

void Handle_Client_Event(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);
bool Connect_Client(std::string client);
void Disconnect_Client(std::string client);
bool Is_Client_Connected(std::string client);
bool Update_Client_TTL(RedisModuleCtx *ctx , bool first_update = false);
void Client_TTL_Handler(RedisModuleCtx *ctx, std::unordered_map<std::string, unsigned long long> &client2ttl , std::unordered_map<std::string, bool> &client2online);
void Start_Client_Handler(RedisModuleCtx *ctx);

#endif /* CLIENT_TRACKER_H */