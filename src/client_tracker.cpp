#include "client_tracker.h"
#include "logger.h"
#include "constants.h"

void handleClientEvent(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                    uint64_t subevent, void *data) {
    if (data == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Handle_Client_Event failed with NULL data.");
        return;
    }

    RedisModuleClientInfo *client_info = (RedisModuleClientInfo*)data;
    unsigned long long client_id = client_info->id;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Handle_Client_Event client : " + std::to_string(client_id));
    ClientTracker& client_tracker = ClientTracker::getInstance();
    std::string client_name = client_tracker.getClientNameFromID(ctx, client_id);
    
    if (eid.id == REDISMODULE_EVENT_CLIENT_CHANGE) {
        switch (subevent) {
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED:
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Handle_Client_Event client disconnected : " + client_name);
                if (!client_name.empty()) {
                    if(client_tracker.getExpirationThreadRunning() == false) {   
                        client_tracker.disconnectClient(ctx, client_name);
                    } else {
                        client_tracker.addClientToDisconnect(client_name);
                    }
                }
                break;
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED:
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Handle_Client_Event client connected : " + client_name);
                break;
        }
    }
}

void ClientTracker::addToClientTrackingGroup(RedisModuleCtx *ctx, const std::string& client_tracking_group, const std::string& client) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Add_To_Client_Tracking_Group client  : " + client + " group name : " + client_tracking_group);
    if (client_track_group_2_clients.count(client_tracking_group) == 0) {
        client_track_group_2_clients[client_tracking_group] = std::set<std::string>{client};
    } else {
        client_track_group_2_clients[client_tracking_group].insert(client);
    }
    client_2_client_track_group[client] = client_tracking_group;
}

std::string ClientTracker::getClientClientTrackingGroup(const std::string& client) {
    if (client_2_client_track_group.count(client) == 0) {
        return "";
    }
    return client_2_client_track_group[client];
}

std::set<std::string> ClientTracker::getClientTrackingGroupClients(const std::string& client_tracking_group) {
    if (client_track_group_2_clients.count(client_tracking_group) == 0) {
        return std::set<std::string>{};
    }
    return client_track_group_2_clients[client_tracking_group];
}

void ClientTracker::connectClient(RedisModuleCtx *ctx, const std::string& client) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Connect_Client client : " + client);
    client_connection[client] = true;
}

void ClientTracker::disconnectClient(RedisModuleCtx *ctx, const std::string& client) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Disconnect_Client client : " + client);
    client_connection[client] = false;
    client_connection_timeout.erase(client);
    
    RedisModuleString *client_name = RedisModule_CreateString(ctx, client.c_str(), client.length());
    if (RedisModule_KeyExists(ctx, client_name)) {
        RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
        if (RedisModule_DeleteKey(stream_key) != REDISMODULE_OK) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Disconnect_Client failed to delete the stream.");
        }
    }
    RedisModule_FreeString(ctx, client_name);
}

bool ClientTracker::isClientConnected(const std::string& client) {
    if (client_connection.count(client) == 0) {
        return false;
    }
    return client_connection[client];
}

bool ClientTracker::updateClientTTL(RedisModuleCtx *ctx, bool first_update) {
    std::string client_name = getClientName(ctx);
    if (client_name.empty()) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Update_Client_TTL failed . Client name is empty. ");
        return false;
    } else if (client_connection_timeout.count(client_name) == 0 && first_update == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Update_Client_TTL failed . Client is not registered. ");
        return false;
    }
    
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    unsigned long long ms_value = ms.count();
    client_connection_timeout[client_name] = ms_value;
    return true;
}

std::string ClientTracker::getClientName(RedisModuleCtx *ctx) {
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    return getClientNameFromID(ctx, client_id);
}

std::string ClientTracker::getClientNameFromID(RedisModuleCtx *ctx, unsigned long long client_id) {
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id);
    if (client_name == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Get_Client_Name_From_ID : Failed to get client name.");
        return "";
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if (client_name_str.empty()) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Get_Client_Name_From_ID : Failed to get client name because client name is not set.");
        return "";
    }
    return client_name_str;
}

void ClientTracker::startClientHandler(RedisModuleCtx *ctx) {
    std::thread client_checker_thread([this, ctx]() { this->clientTTLHandler(ctx); });
    client_checker_thread.detach();
}

void ClientTracker::clientTTLHandler(RedisModuleCtx *ctx) {
    
    while(true) {
        auto now = std::chrono::system_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
        unsigned long long now_ms_value = ms.count();
        std::vector<std::string> expire_client_list;
        setExpirationThreadRunning(true);
        for(const auto& pair : client_connection_timeout) {
            unsigned long long diff_in_ms = now_ms_value - pair.second;
            if(diff_in_ms >= (unsigned long long)(cct_config.CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CFG * MS_MULT * cct_config.CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CFG)) {
                expire_client_list.push_back(pair.first);
            }
        }

        for(const auto& client : clients_to_disconnect) {
            expire_client_list.push_back(client);
        }
        
        for(const auto& client : expire_client_list) {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Client_TTL_Handler expire client : " + client);
            disconnectClient(ctx, client);
        }
        setExpirationThreadRunning(false);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void ClientTracker::setClientQueryTTL(RedisModuleCtx *ctx, const std::string& client, unsigned long long ttl) {
    client_query_ttl[client] = ttl;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Set_Client_Query_TTL client  : " + client + " , TTL: " + std::to_string(ttl));
}

unsigned long long ClientTracker::getClientQueryTTL(const std::string& client) {
    if (client_query_ttl.count(client) == 0) {
        return (cct_config.CCT_QUERY_TTL_SECOND_CFG * MS_MULT);
    }
    return client_query_ttl[client];
}