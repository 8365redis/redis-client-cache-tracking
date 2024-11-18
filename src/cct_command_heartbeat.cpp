#include <errno.h>
#include <string.h>
#include <vector>
#include <string>
#include <unordered_map>

#include "cct_command_heartbeat.h"
#include "logger.h"
#include "constants.h"
#include "cct_client_tracker.h" 
#include "cct_query_tracking_data.h"
#include "query_parser.h"
int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    ClientTracker& client_tracker = ClientTracker::getInstance();

    RedisModuleString *client_name_from_argv = NULL;
    std::string client_name;
    FindAndRemoveClientName(argv, &argc, &client_name_from_argv);
    if(client_name_from_argv != NULL) {
        client_name = RedisModule_StringPtrLen(client_name_from_argv, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Heartbeat_RedisCommand CLIENTNAME is provided in argv: " + client_name );
    } else {
        client_name = client_tracker.getClientName(ctx);
    }

    if (client_tracker.isClientConnected(client_name) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed : Client is not registered : " + client_name );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    // Regardless update the TTL
    if (client_tracker.updateClientTTL(ctx, false, client_name) == false) {
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