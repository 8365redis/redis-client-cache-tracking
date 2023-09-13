#include "client_tracker.h"
#include "logger.h"
#include "constants.h"

void handleClientEvent(RedisModuleCtx *ctx, RedisModuleEvent eid,
                       uint64_t subevent, void *data) {

    if (data == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "handleClientEvent failed with NULL data." );
        return;
    }

    RedisModuleClientInfo *client_info = (RedisModuleClientInfo*)data;
    unsigned long long client_id = client_info->id;
    std::string client_name = Get_Client_Name_From_ID(ctx, client_id);
    if (eid.id == REDISMODULE_EVENT_CLIENT_CHANGE) {
        switch (subevent) {
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED: {
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "handleClientEvent client disconnected : " +  client_name );
            } 
            break;

            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED: {
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "handleClientEvent client connected : " +  client_name );
            } 
            break;
        }
    }
}


std::string Get_Client_Name(RedisModuleCtx *ctx) {
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    return Get_Client_Name_From_ID(ctx, client_id);
}

std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id) {
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id); 
    if ( client_name == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Failed to get client name." );
        return "";
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if ( client_name_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Failed to get client name because client name is not set." );
        return "";
    }
    return client_name_str;
}