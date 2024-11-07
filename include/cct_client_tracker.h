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
    std::atomic<bool> expiration_thread_running;
    std::set<std::string> clients_to_disconnect;
    void clientTTLHandler(RedisModuleCtx* ctx);


public:
    ClientTracker(const ClientTracker&) = delete;
    ClientTracker& operator=(const ClientTracker&) = delete;

    static ClientTracker& getInstance() {
        static ClientTracker instance;
        return instance;
    }
    void setExpirationThreadRunning(bool value) { expiration_thread_running = value; }
    bool getExpirationThreadRunning() const { return expiration_thread_running; }
    void addClientToDisconnect(const std::string& client) { clients_to_disconnect.insert(client); }
    
    void addToClientTrackingGroup(RedisModuleCtx* ctx, const std::string& client_tracking_group, const std::string& client);
    std::string getClientClientTrackingGroup(const std::string& client);
    std::set<std::string> getClientTrackingGroupClients(const std::string& client_tracking_group);
    void connectClient(RedisModuleCtx* ctx, const std::string& client);
    void disconnectClient(RedisModuleCtx* ctx, const std::string& client);
    bool isClientConnected(const std::string& client);
    bool updateClientTTL(RedisModuleCtx* ctx, bool first_update);
    std::string getClientName(RedisModuleCtx* ctx);
    std::string getClientNameFromID(RedisModuleCtx* ctx, unsigned long long client_id);
    void startClientHandler(RedisModuleCtx* ctx);
    void setClientQueryTTL(RedisModuleCtx* ctx, const std::string& client, unsigned long long ttl);
    unsigned long long getClientQueryTTL(const std::string& client);
};


void handleClientEvent(RedisModuleCtx* ctx, RedisModuleEvent eid, uint64_t subevent, void* data);