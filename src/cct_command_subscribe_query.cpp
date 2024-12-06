#include "cct_command_subscribe_query.h"
#include "constants.h"
#include "cct_client_tracker.h"
#include "query_parser.h"
#include "logger.h"

int Subscribe_Query_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Subscribe_Query_RedisCommand is called");
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    if(argv[1] == NULL || argv[2] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Subscribe_Query_RedisCommand failed to execute query because query is NULL.");
        return REDISMODULE_ERR;
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();

    RedisModuleString *client_name_from_argv = NULL;
    FindAndRemoveClientName(argv, &argc, &client_name_from_argv);
    std::string client_name_str;
    if(client_name_from_argv != NULL) {
        client_name_str = RedisModule_StringPtrLen(client_name_from_argv, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Subscribe_Query_RedisCommand CLIENTNAME is provided in argv: " + client_name_str);
    } else {
        client_name_str = client_tracker.getClientName(ctx);
    }

    if (client_tracker.isClientConnected(client_name_str) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Subscribe_Query_RedisCommand failed : Client is not registered" );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    // Convert argv to vector of strings
    std::vector<std::string> query_args;
    for (int i = 1; i < argc; i++) {
        query_args.push_back(RedisModule_StringPtrLen(argv[i], NULL));
    }

    //print query_args in one line
    std::string query_str = "";
    for (const auto& arg : query_args) {
        query_str += arg + " ";
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Subscribe_Query_RedisCommand query_str: " + query_str);
    

    return REDISMODULE_OK;
}
