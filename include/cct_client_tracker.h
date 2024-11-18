#pragma once
#include <string>
#include <unordered_map>
#include <set>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include "redismodule.h"


class ClientTracker {
private:
    ClientTracker() = default;
    
    std::unordered_map<std::string, bool> client_connection;
    std::unordered_map<std::string, unsigned long long> client_connection_timeout;
    std::unordered_map<std::string, unsigned long long> client_query_ttl;
    std::unordered_map<std::string, std::set<std::string>> client_track_group_2_clients;
    std::unordered_map<std::string, std::string> client_2_client_track_group;
    void clientTTLHandler(RedisModuleCtx* ctx);


public:
    ClientTracker(const ClientTracker&) = delete;
    ClientTracker& operator=(const ClientTracker&) = delete;

    static ClientTracker& getInstance() {
        static ClientTracker instance;
        return instance;
    }
    
    void addToClientTrackingGroup(RedisModuleCtx* ctx, const std::string& client_tracking_group, const std::string& client);
    std::string getClientClientTrackingGroup(const std::string& client);
    std::set<std::string> getClientTrackingGroupClients(const std::string& client_tracking_group);
    void connectClient(RedisModuleCtx* ctx, const std::string& client);
    void disconnectClient(RedisModuleCtx* ctx, const std::string& client);
    bool isClientConnected(const std::string& client);
    bool updateClientTTL(RedisModuleCtx* ctx, bool first_update, std::string client_name);
    std::string getClientName(RedisModuleCtx* ctx);
    std::string getClientNameFromID(RedisModuleCtx* ctx, unsigned long long client_id);
    void startClientHandler(RedisModuleCtx* ctx);
    void setClientQueryTTL(RedisModuleCtx* ctx, const std::string& client, unsigned long long ttl);
    unsigned long long getClientQueryTTL(const std::string& client);
};