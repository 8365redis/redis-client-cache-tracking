#include <mutex>
#include <iomanip>
#include <sstream>
#include <queue>
#include "cct_command_subscribe_index.h"
#include "constants.h"
#include "cct_client_tracker.h"
#include "query_parser.h"
#include "logger.h"
#include "cct_query_tracking_data.h"
#include "cct_index_tracker.h"

#ifdef _DEBUG
const long unsigned int DEFAULT_CHUNK_SIZE = 2;
#else
const long unsigned int DEFAULT_CHUNK_SIZE = 1000;
#endif

std::unordered_map<std::string, std::atomic_bool> index_in_setup;
typedef struct Key_Space_Notification {
    std::string index_name;
    std::string event;
    std::string key;
} Key_Space_Notification;
std::unordered_map<std::string, std::queue<Key_Space_Notification>> key_space_notification_queue;


void Set_Index_In_Setup(RedisModuleCtx *ctx, std::string index_name, bool value) {
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Set_Index_In_Setup is called for index: " + index_name + " with value: " + std::to_string(value));
    if(value == false) {
        Process_Queued_Key_Space_Notifications(ctx, index_name);
    }
    index_in_setup[index_name] = value;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Set_Index_In_Setup is finished for index: " + index_name + " with value: " + std::to_string(value));
}

bool Is_Index_In_Setup(std::string index_name) {
    return index_in_setup[index_name];
}

void Queue_Key_Space_Notification(std::string index_name, std::string event, std::string key) {
    if (key_space_notification_queue.find(index_name) == key_space_notification_queue.end()) {
        key_space_notification_queue[index_name] = std::queue<Key_Space_Notification>();
    }
    key_space_notification_queue[index_name].push({index_name, event, key});
}

void Process_Queued_Key_Space_Notifications(RedisModuleCtx *ctx, std::string index_name) {
    if (key_space_notification_queue.find(index_name) == key_space_notification_queue.end()) {
        return;
    }
    // Iterate over the queue and process each notification
    while (!key_space_notification_queue[index_name].empty()) {
        Key_Space_Notification notification = key_space_notification_queue[index_name].front();
        key_space_notification_queue[index_name].pop();
        Add_Tracked_Index_Event_To_Stream(ctx, notification.index_name, notification.event, notification.key);
    }
    key_space_notification_queue.erase(index_name);
}

int Repopulate_Index_Stream(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Repopulate_Index_Stream is called");
    std::string index_name = RedisModule_StringPtrLen(argv[1], NULL);   
    
    if(!Is_Index_Tracked(index_name)) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Repopulate_Index_Stream failed : Index is not tracked" );
        return RedisModule_ReplyWithError(ctx, "Index not supported");
    }

    if(Is_Index_In_Setup(index_name)) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Repopulate_Index_Stream failed : Index is being setup" );
        RedisModule_ReplyWithError(ctx, "Index is being setup");
        return REDISMODULE_OK;
    }

    RedisModule_Call(ctx, "DEL", "c", index_name.c_str());
    RedisModuleCtx *thread_safe_ctx = RedisModule_GetDetachedThreadSafeContext(ctx);
    Start_Index_Subscription_Handler(thread_safe_ctx, index_name, DEFAULT_CHUNK_SIZE);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Repopulate_Index_Stream is finished");

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int Disable_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Disable_Index_Subscription is called");
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }
    std::string index_name = RedisModule_StringPtrLen(argv[1], NULL);

    if(Is_Index_In_Setup(index_name) ) {
        RedisModule_ReplyWithError(ctx, "Index is being setup, try again later");
        return REDISMODULE_OK;
    }

    Remove_Subscribed_Index(ctx, index_name);
    UnTrack_Index(ctx, index_name);
    RedisModule_Call(ctx, "DEL", "c", index_name.c_str());
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int Enable_Index_Subscription(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Enable_Index_Subscription is called");
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    if(argv[1] == NULL){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Enable_Index_Subscription failed to execute because index is NULL.");
        return REDISMODULE_ERR;
    }

    RedisModuleString *index_name = argv[1];
    std::string index_name_str = RedisModule_StringPtrLen(index_name, NULL);

    if( RedisModule_KeyExists(ctx, index_name) ) {
        RedisModule_ReplyWithError(ctx, "Index already setup");
        return REDISMODULE_OK;
    }

    if(Is_Index_In_Setup(index_name_str)) {
        RedisModule_ReplyWithError(ctx, "Index is being setup");
        return REDISMODULE_OK;
    }

    RedisModuleCtx *thread_safe_ctx = RedisModule_GetDetachedThreadSafeContext(ctx);
    Start_Index_Subscription_Handler(thread_safe_ctx, index_name_str, DEFAULT_CHUNK_SIZE);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

void Start_Index_Subscription_Handler(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Index_Subscription_Handler is called");
    std::thread t(Process_Index_Subscription, ctx, index_name, chunk_size);
    t.detach();
}

void Process_Index_Subscription(RedisModuleCtx *ctx, std::string index_name, long unsigned int chunk_size) {
    RedisModule_AutoMemory(ctx);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription :" + index_name + " with chunk_size: " + std::to_string(chunk_size) + " index_name: " + index_name);

    Set_Index_In_Setup(ctx, index_name, true);
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
    bool subscribed_index_stream_added = false;
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
                if ( REDISMODULE_OK == Add_Event_To_Stream(ctx, index_name, "json.set", keys_chunks[i], get_reply_elem_str, index_name, false, true) && !subscribed_index_stream_added) {
                    Add_Subscribed_Index(ctx, index_name);
                    Track_Index(ctx, index_name);
                    subscribed_index_stream_added = true;
                }
                RedisModule_ThreadSafeContextUnlock(ctx);
            }
            keys_chunks.clear();
        }
    }

    Set_Index_In_Setup(ctx, index_name, false);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Process_Index_Subscription is finished");
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

    if(!Is_Index_Tracked(index_name_str)) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Subscribe_Index_RedisCommand failed : Index is not tracked" );
        return RedisModule_ReplyWithError(ctx, "Index not supported");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, index_name_str.c_str());
    }

    return REDISMODULE_OK;
}