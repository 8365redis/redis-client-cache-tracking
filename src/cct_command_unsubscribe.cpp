#include "cct_command_unsubscribe.h"
#include "cct_query_expiry_logic.h"
#include "cct_client_tracker.h"
#include "query_parser.h"
#include "logger.h"
#include "constants.h"
#include <string>

int Unsubscribe_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Unsubscribe_Command is called");
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    if(argv[1] == NULL || argv[2] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Unsubscribe_Command failed to execute query because query is NULL.");
        return REDISMODULE_ERR;
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();

    RedisModuleString *client_name_from_argv = NULL;
    FindAndRemoveClientName(argv, &argc, &client_name_from_argv);
    std::string client_name_str;
    if (client_name_from_argv != NULL) {
        client_name_str = RedisModule_StringPtrLen(client_name_from_argv, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Unsubscribe_Command CLIENTNAME is provided in argv: " + client_name_str);
    } else {
        client_name_str = client_tracker.getClientName(ctx);
    }

    RedisModuleString *index = argv[1];
    RedisModuleString *query = argv[2];
    std::string query_str = RedisModule_StringPtrLen(query, NULL);
    std::string index_str = RedisModule_StringPtrLen(index, NULL);

    std::string query_term_attribute_normalized;
    if(query_str == WILDCARD_SEARCH) {
        query_term_attribute_normalized = WILDCARD_SEARCH;
    } else {
        query_term_attribute_normalized = Get_Query_Normalized(query);
    }
    std::string index_and_query = index_str + CCT_MODULE_KEY_SEPERATOR + query_term_attribute_normalized;

    Clean_Up_Query(ctx , client_name_str, index_and_query, true);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}