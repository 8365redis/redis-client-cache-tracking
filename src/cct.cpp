#include "redismodule.h"
#include "logger.h"

#include "cct_query_tracking_logic.h"
#include "cct_command_register.h"
#include "cct_command_search.h"
#include "cct_command_heartbeat.h"
#include "cct_command_unsubscribe.h"
#include "cct_offline_query_expire.h"
#include "constants.h"
#include "config_handler.h"
#include "version.h"
#include "cct_index_tracker.h"
#include "cct_client_tracker.h"
#include "cct_command_filter.h"
#include "cct_command_renew.h"
#include "cct_command_subscribe_index.h"
#ifndef CCT_MODULE_VERSION
#define CCT_MODULE_VERSION "unknown"
#endif

CCT_Config cct_config;

RedisModuleCtx *rdts_staticCtx;

#ifdef __cplusplus
extern "C" {
#endif



int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    int version_int = Get_Module_Version(CCT_MODULE_VERSION);

    const char* version_string = { CCT_MODULE_VERSION " compiled at " __TIME__ " "  __DATE__  };
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "CCT_MODULE_VERSION : " + std::string(version_string));

    #ifdef _DEBUG
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "THIS IS A DEBUG BUILD." );
    #endif
    #ifdef NDEBUG
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "THIS IS A RELEASE BUILD." );
    #endif 

    if (RedisModule_Init(ctx,"CCT2",version_int,REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    std::string config_file_path_str = "";
    if (argc > 0) {
        config_file_path_str = RedisModule_StringPtrLen(argv[0], NULL);
    }
    #ifdef _DEBUG
    config_file_path_str = "non-existing-file";  // DEBUG version will always use hardcoded default DEBUG values
    #endif
    cct_config = Read_CCT_Config(ctx, config_file_path_str);


    //if ( Handle_Offline_Query_Expire(ctx) == REDISMODULE_ERR){
    //    return REDISMODULE_ERR;
    //}

    rdts_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

    if (RedisModule_CreateCommand(ctx,"CCT2.REGISTER", Register_RedisCommand , "admin write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.REGISTER command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.FT.SEARCH", FT_Search_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.FT.SEARCH command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.HEARTBEAT", Heartbeat_RedisCommand , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.HEARTBEAT command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.FT.SEARCH.UNSUBSCRIBE", Unsubscribe_Command , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.FT.SEARCH.UNSUBSCRIBE command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.FT.RENEW", Renew_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.FT.RENEW command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.SUBSCRIBE_TO_INDEX", Subscribe_Index_RedisCommand , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.SUBSCRIBE_TO_INDEX command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.ENABLE_INDEX_SUBSCRIPTION", Enable_Index_Subscription , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.ENABLE_INDEX_SUBSCRIPTION command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.DISABLE_INDEX_SUBSCRIPTION", Disable_Index_Subscription , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.DISABLE_INDEX_SUBSCRIPTION command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"CCT2.REPOPULATE_INDEX_STREAM", Repopulate_Index_Stream , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "CCT2.REPOPULATE_INDEX_STREAM command created successfully.");
    }

    // Subscribe to key space events
    if ( RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_SET | REDISMODULE_NOTIFY_STRING |
            REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_NEW | REDISMODULE_NOTIFY_MODULE ,
             Notify_Callback) != REDISMODULE_OK ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "RedisModule_OnLoad failed to SubscribeToKeyspaceEvents." );
        return RedisModule_ReplyWithError(ctx, "SubscribeToKeyspaceEvents has failed");
    }

    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading,
                                             OnRedisReady) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "RedisModule_OnLoad failed to SubscribeToServerEvent for RedisModuleEvent_Loading." );
        return RedisModule_ReplyWithError(ctx, "SubscribeToServerEvent to Loading has failed");
    }  

    RedisModule_RegisterCommandFilter(ctx, Command_Filter_Callback, REDISMODULE_CMDFILTER_NOSELF);

    ClientTracker& client_tracker = ClientTracker::getInstance();
    client_tracker.startClientHandler(rdts_staticCtx);

    Start_Index_Change_Handler(rdts_staticCtx);

    Start_Snapshot_Handler(rdts_staticCtx);
  
    return REDISMODULE_OK;
}


#ifdef __cplusplus
}
#endif

