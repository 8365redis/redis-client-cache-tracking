#include "client_tracker.h"
#include "logger.h"
#include "constants.h"

std::unordered_map<std::string, bool> CCT_CLIENT_CONNECTION;
std::unordered_map<std::string, unsigned long long> CCT_CLIENT_CONNECTION_TIMEOUT;
std::unordered_map<std::string, unsigned long long> CCT_CLIENT_QUERY_TTL;

void Handle_Client_Event(RedisModuleCtx *ctx, RedisModuleEvent eid,
                       uint64_t subevent, void *data) {

    if (data == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Client_Event failed with NULL data." );
        return;
    }

    RedisModuleClientInfo *client_info = (RedisModuleClientInfo*)data;
    unsigned long long client_id = client_info->id;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Client_Event client : " + std::to_string(client_id) );
    std::string client_name = Get_Client_Name_From_ID(ctx, client_id);
    if (eid.id == REDISMODULE_EVENT_CLIENT_CHANGE) {
        switch (subevent) {
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED: 
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Client_Event client disconnected : " + client_name );
                if (!client_name.empty()) {
                    Disconnect_Client(ctx, client_name);
                }
                break;
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED: 
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handle_Client_Event client connected : " + client_name);
                break;
        }
    }
}

void Connect_Client(std::string client) {
    CCT_CLIENT_CONNECTION[client] = true;
}

void Disconnect_Client(RedisModuleCtx *ctx, std::string client) {
    CCT_CLIENT_CONNECTION[client] = false;
    RedisModuleString *client_name = RedisModule_CreateString(ctx, client.c_str(), client.length());
    CCT_CLIENT_CONNECTION_TIMEOUT.erase(client);
    // Check if the stream exists and delete if it is
    if( RedisModule_KeyExists(ctx, client_name) ) { // NOT checking if it is stream
        RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
        if (RedisModule_DeleteKey(stream_key) != REDISMODULE_OK ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Disconnect_Client failed to delete the stream." );
        }
    }
    RedisModule_FreeString(ctx, client_name);
}

bool Is_Client_Connected(std::string client) {
    if (CCT_CLIENT_CONNECTION.count(client) == 0){
        return false;
    }
    return CCT_CLIENT_CONNECTION[client] ;
}

bool Update_Client_TTL(RedisModuleCtx *ctx, bool first_update ) {
    std::string client_name = Get_Client_Name(ctx);
    if ( client_name.empty() ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Update_Client_TTL failed . Client name is empty. ");
        return false;
    }else if ( CCT_CLIENT_CONNECTION_TIMEOUT.count(client_name) == 0 && first_update == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Update_Client_TTL failed . Client is not registered. ");
        return false;
    }
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    unsigned long long ms_value = ms.count();
    CCT_CLIENT_CONNECTION_TIMEOUT[client_name] = ms_value;
    return true;
}

std::string Get_Client_Name(RedisModuleCtx *ctx) {
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    return Get_Client_Name_From_ID(ctx, client_id);
}

std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id) {
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id); 
    if ( client_name == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Client_Name_From_ID : Failed to get client name." );
        return "";
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if ( client_name_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Client_Name_From_ID : Failed to get client name because client name is not set." );
        return "";
    }
    return client_name_str;
}

void Start_Client_Handler(RedisModuleCtx *ctx) {

    std::thread client_checker_thread(Client_TTL_Handler, ctx, std::ref(CCT_CLIENT_CONNECTION_TIMEOUT), std::ref(CCT_CLIENT_CONNECTION));
    client_checker_thread.detach();
}

void Client_TTL_Handler(RedisModuleCtx *ctx, std::unordered_map<std::string, unsigned long long> &client2ttl, std::unordered_map<std::string, bool> &client2online) {

    while(true) {
        auto now = std::chrono::system_clock::now();
        auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
        unsigned long long now_ms_value = ms.count();
        std::vector<std::string> expire_client_list; 
        for(const auto &pair : client2ttl) {
            unsigned long long diff_in_ms = now_ms_value - pair.second;
            if( diff_in_ms >= CCT_CLIENT_TTL ) {
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Client_TTL_Handler , kill the client : " + pair.first );
                expire_client_list.push_back(pair.first);
            }
        }
        for(const auto &client : expire_client_list) {
            Disconnect_Client(ctx, client);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(CCT_CLIENT_TTL_CHECK_INTERVAL));
    }
}

void Set_Client_Query_TTL(RedisModuleCtx *ctx, std::string client, unsigned long long ttl) {
    CCT_CLIENT_QUERY_TTL[client] = ttl ;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Set_Client_Query_TTL client  : " + client + " , TTL: " + std::to_string(ttl) );
}

unsigned long long Get_Client_Query_TTL(std::string client) {
    if(CCT_CLIENT_QUERY_TTL.count(client) == 0 ){
        return CCT_QUERY_TTL;
    }
    return CCT_CLIENT_QUERY_TTL[client];
}