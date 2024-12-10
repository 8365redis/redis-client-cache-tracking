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

    if(query_args.size() < 2) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Subscribe_Query_RedisCommand failed to execute query because query format is incorrect.");
        RedisModule_ReplyWithSimpleString(ctx, "Query format is incorrect.");
        return REDISMODULE_ERR;
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Subscribe_Query_RedisCommand query_str: " + query_str);
    
    Start_Query_Subscription(ctx, query_args[0], query_args[1], DEFAULT_CHUNK_SIZE);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

void Start_Query_Subscription(RedisModuleCtx *ctx, std::string index_name, std::string query_str, int chunk_size) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Query_Subscription :" + index_name + " with query: " + query_str + " and chunk_size: " + std::to_string(chunk_size));
    std::thread t(Process_Query_Subscription, ctx, index_name, query_str, chunk_size);
    t.detach();
}

void Process_Query_Subscription(RedisModuleCtx *ctx, std::string index_name, std::string query_str, int chunk_size) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Query_Subscription :" + index_name + " with query: " + query_str + " and chunk_size: " + std::to_string(chunk_size));
    unsigned long long cursor_id = 0;
    const std::string with_cursor_count = "WITHCURSOR COUNT";
    const std::string read = "READ";
    const std::string load_key = "LOAD 1 __key";

    do{
        RedisModuleCallReply *reply;
        std::string current_query_str;
        std::vector<RedisModuleString*> arguments;
        if (cursor_id == 0) {
            arguments.push_back(RedisModule_CreateString(ctx, index_name.c_str(), index_name.length()));
            arguments.push_back(RedisModule_CreateString(ctx, query_str.c_str(), query_str.length()));
            arguments.push_back(RedisModule_CreateString(ctx, load_key.c_str(), load_key.length()));
            arguments.push_back(RedisModule_CreateString(ctx, with_cursor_count.c_str(), with_cursor_count.length()));
            arguments.push_back(RedisModule_CreateString(ctx, std::to_string(chunk_size).c_str(), std::to_string(chunk_size).length()));
            current_query_str = "FT.AGGREGATE " + index_name + " " + query_str + " " + load_key + " " + with_cursor_count + " " + std::to_string(chunk_size);
            reply = RedisModule_Call(ctx, "FT.AGGREGATE", "v", arguments.begin(), arguments.size());
        } else {
            arguments.push_back(RedisModule_CreateString(ctx, read.c_str(), read.length()));
            arguments.push_back(RedisModule_CreateString(ctx, index_name.c_str(), index_name.length()));
            arguments.push_back(RedisModule_CreateString(ctx, std::to_string(cursor_id).c_str(), std::to_string(cursor_id).length()));
            arguments.push_back(RedisModule_CreateString(ctx, std::to_string(chunk_size).c_str(), std::to_string(chunk_size).length()));
            current_query_str = "FT.CURSOR READ " + index_name + " " + std::to_string(cursor_id) + " " + std::to_string(chunk_size);
            reply = RedisModule_Call(ctx, "FT.CURSOR", "v", arguments.begin(), arguments.size());
        }

        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Query_Subscription Query : " + current_query_str );

        if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY || RedisModule_CallReplyLength(reply) != 2) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Process_Query_Subscription Query has failed with query: " + current_query_str );
            break;
        }

        RedisModuleCallReply *cursor_reply_data = RedisModule_CallReplyArrayElement(reply, 0);
        if(RedisModule_CallReplyType(cursor_reply_data) != REDISMODULE_REPLY_ARRAY ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Process_Query_Subscription Query has wrong cursor reply data with query: " + current_query_str );
            break;
        }

        RedisModuleCallReply *cursor_replay_id = RedisModule_CallReplyArrayElement(reply, 1);
        if(RedisModule_CallReplyType(cursor_replay_id) != REDISMODULE_REPLY_INTEGER) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Process_Query_Subscription Query has wrong cursor reply id with query: " + current_query_str );
            break;
        }

        cursor_id = RedisModule_CallReplyInteger(cursor_replay_id);\

    } while (cursor_id != 0);
}
