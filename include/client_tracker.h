#ifndef CLIENT_TRACKER_H
#define CLIENT_TRACKER_H

#include "redismodule.h"
#include <unordered_map>
#include <string>
#include <vector>
#include <thread>
#include <set>

void Handle_Client_Event(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);
void Connect_Client(RedisModuleCtx *ctx, std::string client);
void Disconnect_Client(RedisModuleCtx *ctx, std::string client);
bool Is_Client_Connected(std::string client);
bool Update_Client_TTL(RedisModuleCtx *ctx , bool first_update = false);
void Client_TTL_Handler(RedisModuleCtx *ctx, std::unordered_map<std::string, unsigned long long> &client2ttl , std::unordered_map<std::string, bool> &client2online);
void Start_Client_Handler(RedisModuleCtx *ctx);
void Set_Client_Query_TTL(RedisModuleCtx *ctx, std::string client, unsigned long long ttl);
unsigned long long Get_Client_Query_TTL(std::string client);
void Add_To_Client_Tracking_Group(RedisModuleCtx *ctx, std::string client_tracking_group, std::string client);
const std::string Get_Client_Client_Tracking_Group(std::string client);
const std::set<std::string> Get_Client_Tracking_Group_Clients(std::string client_tracking_group);

#endif /* CLIENT_TRACKER_H */