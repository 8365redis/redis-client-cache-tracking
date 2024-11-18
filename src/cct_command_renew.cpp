#include "cct_command_renew.h"
#include "cct_client_tracker.h"
#include "cct_query_tracking_data.h"
#include "logger.h"
#include "query_parser.h"

// CCT2.RENEW command will take list of queries to be renewed (queries are separated by space and prefixed with index)
int Renew_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();

    RedisModuleString *client_name_from_argv = NULL;
    std::string client_name_str;
    FindAndRemoveClientName(argv, &argc, &client_name_from_argv);
    if(client_name_from_argv != NULL) { 
        client_name_str = RedisModule_StringPtrLen(client_name_from_argv, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Renew_RedisCommand CLIENTNAME is provided in argv: " + client_name_str);
    } else {
        client_name_str = client_tracker.getClientName(ctx);
    }

    std::string client_tracking_group = client_tracker.getClientClientTrackingGroup(client_name_str);
    unsigned long long client_ttl = client_tracker.getClientQueryTTL(client_tracking_group);

    std::vector<std::string> queries;
    for (int i = 1; i < argc; i++) {
        std::string query = RedisModule_StringPtrLen(argv[i], NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Renew_RedisCommand : Renewing query: " + query);
        queries.push_back(query);
    }

    Renew_Queries(ctx, queries, client_tracking_group, client_ttl);

    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}