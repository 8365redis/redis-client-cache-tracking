#include <mutex>
#include "cct_command_subscribe_index.h"
#include "constants.h"
#include "cct_client_tracker.h"
#include "query_parser.h"
#include "logger.h"
#include "cct_query_tracking_data.h"

const long unsigned int DEFAULT_CHUNK_SIZE = 2;
std::set<std::string> subscribed_indexes;
std::set<std::string> subscribed_indexes_to_process;
std::set<std::string> subscribed_indexes_to_process_buffer;
std::mutex subscribed_indexes_to_process_mutex;

int Setup_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Setup_Index_Subscription is called");
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    if(argv[1] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Setup_Index_Subscription failed to execute because index is NULL.");
        return REDISMODULE_ERR;
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();

    RedisModuleString *client_name_from_argv = NULL;
    FindAndRemoveClientName(argv, &argc, &client_name_from_argv);
    std::string client_name_str;
    if(client_name_from_argv != NULL) {
        client_name_str = RedisModule_StringPtrLen(client_name_from_argv, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Setup_Index_Subscription CLIENTNAME is provided in argv: " + client_name_str);
    } else {
        client_name_str = client_tracker.getClientName(ctx);
    }

    if (client_tracker.isClientConnected(client_name_str) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Setup_Index_Subscription failed : Client is not registered" );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    // Convert argv to vector of strings
    RedisModuleString *index_name = argv[1];
    std::string index_name_str = RedisModule_StringPtrLen(index_name, NULL);

    if( RedisModule_KeyExists(ctx, index_name) ) {
        RedisModule_ReplyWithError(ctx, "Index already setup");
        return REDISMODULE_OK;
    }   

    subscribed_indexes_to_process_mutex.lock();
    subscribed_indexes_to_process_buffer.insert(index_name_str);
    subscribed_indexes_to_process_mutex.unlock();

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int Subscribe_Index_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Subscribe_Index_RedisCommand is called");
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    if(argv[1] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Subscribe_Index_RedisCommand failed to execute because index is NULL.");
        return REDISMODULE_ERR;
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();

    RedisModuleString *client_name_from_argv = NULL;
    FindAndRemoveClientName(argv, &argc, &client_name_from_argv);
    std::string client_name_str;
    if(client_name_from_argv != NULL) {
        client_name_str = RedisModule_StringPtrLen(client_name_from_argv, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Subscribe_Index_RedisCommand CLIENTNAME is provided in argv: " + client_name_str);
    } else {
        client_name_str = client_tracker.getClientName(ctx);
    }

    if (client_tracker.isClientConnected(client_name_str) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Subscribe_Index_RedisCommand failed : Client is not registered" );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    std::string index_name_str = RedisModule_StringPtrLen(argv[1], NULL);

    if( subscribed_indexes.find(index_name_str) != subscribed_indexes.end() ) {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }else {
        RedisModule_ReplyWithError(ctx, "Index does not exist");
    }
    return REDISMODULE_OK;
}

void Start_Index_Subscription_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Index_Subscription_Handler is called");
    std::thread t(Index_Subscription_Handler, ctx);
    t.detach();
}

void Index_Subscription_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Index_Subscription_Handler is called");

    while(true) {
        for (auto index_name : subscribed_indexes_to_process) {
            Process_Index_Subscription(ctx, index_name, DEFAULT_CHUNK_SIZE);
            subscribed_indexes.insert(index_name);
        }
        subscribed_indexes_to_process.clear();
        subscribed_indexes_to_process_mutex.lock();
        subscribed_indexes_to_process.insert(subscribed_indexes_to_process_buffer.begin(), subscribed_indexes_to_process_buffer.end());
        subscribed_indexes_to_process_buffer.clear();
        subscribed_indexes_to_process_mutex.unlock();
    }
}


void Process_Index_Subscription(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription :" + index_name + " with chunk_size: " + std::to_string(chunk_size));
    unsigned long long cursor_id = 0;
    const std::string with_cursor = "WITHCURSOR";
    const std::string count = "COUNT";
    const std::string read = "READ";
    const std::string load = "LOAD";
    const std::string one = "1";
    const std::string key = "__key";
    const std::string query_str = "*";

    std::string current_query_str;
    std::vector<std::string> keys;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription is started");
    do{
        RedisModuleCallReply *reply;
        std::vector<RedisModuleString*> arguments;
        if (cursor_id == 0) {
            arguments.push_back(RedisModule_CreateString(ctx, index_name.c_str(), index_name.length()));
            arguments.push_back(RedisModule_CreateString(ctx, query_str.c_str(), query_str.length()));
            arguments.push_back(RedisModule_CreateString(ctx, load.c_str(), load.length()));
            arguments.push_back(RedisModule_CreateString(ctx, one.c_str(), one.length()));
            arguments.push_back(RedisModule_CreateString(ctx, key.c_str(), key.length()));
            arguments.push_back(RedisModule_CreateString(ctx, with_cursor.c_str(), with_cursor.length()));
            arguments.push_back(RedisModule_CreateString(ctx, count.c_str(), count.length()));
            arguments.push_back(RedisModule_CreateString(ctx, std::to_string(chunk_size).c_str(), std::to_string(chunk_size).length()));
            current_query_str = "FT.AGGREGATE " + index_name + " " + query_str + " " + load + " " + one + " " + key + " " + with_cursor + " " + count + " " + std::to_string(chunk_size);
            RedisModule_ThreadSafeContextLock(ctx);
            reply = RedisModule_Call(ctx, "FT.AGGREGATE", "v", arguments.data(), arguments.size());
            RedisModule_ThreadSafeContextUnlock(ctx);
        } else {
            arguments.push_back(RedisModule_CreateString(ctx, read.c_str(), read.length()));
            arguments.push_back(RedisModule_CreateString(ctx, index_name.c_str(), index_name.length()));
            arguments.push_back(RedisModule_CreateString(ctx, std::to_string(cursor_id).c_str(), std::to_string(cursor_id).length()));
            arguments.push_back(RedisModule_CreateString(ctx, std::to_string(chunk_size).c_str(), std::to_string(chunk_size).length()));
            current_query_str = "FT.CURSOR READ " + index_name + " " + std::to_string(cursor_id) + " " + std::to_string(chunk_size);
            RedisModule_ThreadSafeContextLock(ctx);
            reply = RedisModule_Call(ctx, "FT.CURSOR", "v", arguments.data(), arguments.size());
            RedisModule_ThreadSafeContextUnlock(ctx);
        }

        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription Query : " + current_query_str );

        if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Process_Index_Subscription Query has failed with query: " + current_query_str );
            break;
        }

        RedisModuleCallReply *cursor_reply_data = RedisModule_CallReplyArrayElement(reply, 0);
        if(RedisModule_CallReplyType(cursor_reply_data) != REDISMODULE_REPLY_ARRAY ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Process_Index_Subscription Query has wrong cursor reply data with query: " + current_query_str );
            break;
        }

        RedisModuleCallReply *cursor_replay_id = RedisModule_CallReplyArrayElement(reply, 1);
        if(RedisModule_CallReplyType(cursor_replay_id) != REDISMODULE_REPLY_INTEGER) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Process_Index_Subscription Query has wrong cursor reply id with query: " + current_query_str );
            break;
        }

        cursor_id = RedisModule_CallReplyInteger(cursor_replay_id);

        size_t data_reply_length = RedisModule_CallReplyLength(cursor_reply_data);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription Query has data reply length: " + std::to_string(data_reply_length) );

        
        for (size_t i = 1; i < data_reply_length; i++) {
            RedisModuleCallReply *data_reply = RedisModule_CallReplyArrayElement(cursor_reply_data, i);
            if (RedisModule_CallReplyType(data_reply) == REDISMODULE_REPLY_ARRAY){
                size_t inner_data_reply_length = RedisModule_CallReplyLength(data_reply);
                if (inner_data_reply_length == 2){  
                    RedisModuleCallReply *inner_key_reply = RedisModule_CallReplyArrayElement(data_reply, 1);
                    RedisModuleString *response = RedisModule_CreateStringFromCallReply(inner_key_reply);
                    const char *response_str = RedisModule_StringPtrLen(response, NULL);
                    keys.push_back(response_str);
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription Query has key: " + std::string(response_str) );
                } else {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Process_Index_Subscription Query has wrong data reply length with query: " + current_query_str );
                }
            }
        }

    } while (cursor_id != 0);

    std::vector<std::string> keys_chunks;
    for (size_t i = 0; i < keys.size(); i++) {
        keys_chunks.push_back(keys[i]);
        if (keys_chunks.size() == chunk_size || i == keys.size() - 1) {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription Query has keys sub array: " + Vector_To_String(keys_chunks, ",") );

            long unsigned int get_argc = keys_chunks.size();
            RedisModuleString **get_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * get_argc);
            for (long unsigned int i = 0; i < get_argc; ++i) {
                get_argv[i] = RedisModule_CreateString(ctx, keys_chunks[i].c_str(), keys_chunks[i].size());
            }
            RedisModule_ThreadSafeContextLock(ctx);
            RedisModuleCallReply *get_reply = RedisModule_Call(ctx, "JSON.MGET", "vc", get_argv , get_argc, "$");
            RedisModule_ThreadSafeContextUnlock(ctx);
            if (RedisModule_CallReplyType(get_reply) != REDISMODULE_REPLY_ARRAY) {
                RedisModule_Free(get_argv);
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "[ERROR]  Process_Index_Subscription JSON.MGET returned not an array." );
                continue;
            }
            RedisModule_Free(get_argv);

            // Parse get values
            const size_t get_reply_length = RedisModule_CallReplyLength(get_reply);
            if(get_reply_length != keys_chunks.size()) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "[ERROR] Process_Index_Subscription JSON.MGET returned array size is not right. Reply : " +  std::to_string(get_reply_length)  );
                continue;
            }
            for (size_t i = 0; i < get_reply_length; i++) {  
                RedisModuleCallReply *get_inner_reply = RedisModule_CallReplyArrayElement(get_reply, i);
                RedisModuleString *get_reply_elem = RedisModule_CreateStringFromCallReply(get_inner_reply);
                const char *get_reply_elem_str = RedisModule_StringPtrLen(get_reply_elem, NULL);
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription key: " + std::string(keys_chunks[i]) + " value: " + get_reply_elem_str );
                RedisModule_ThreadSafeContextLock(ctx);
                Add_Event_To_Stream(ctx, index_name, "json.set", keys_chunks[i], get_reply_elem_str, index_name, false, true);
                RedisModule_ThreadSafeContextUnlock(ctx);
            }

            keys_chunks.clear();
        }
    }


    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription is finished");
}