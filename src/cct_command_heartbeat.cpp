#include <errno.h>
#include <string.h>
#include <vector>
#include <string>
#include <unordered_map>

#include "cct_command_heartbeat.h"
#include "logger.h"
#include "constants.h"
#include "client_tracker.h"
#include "cct_query_tracking_data.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc > 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();
    std::string client_name = client_tracker.getClientName(ctx);

    if (client_tracker.isClientConnected(client_name) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed : Client is not registered : " + client_name );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    // Regardless update the TTL
    if (client_tracker.updateClientTTL(ctx, false) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed.");
        return RedisModule_ReplyWithError(ctx, "Updating the TTL failed");
    }

    // Check if we have to trim the stream
    if (argc == 2) {
        if ( Trim_Stream_By_ID(ctx, argv[1], client_name) == REDISMODULE_ERR ){
            return RedisModule_ReplyWithError(ctx, "Trim with given Stream ID failed");
        }
    } 
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}