#include <unordered_map>
#include <thread>
#include "cct_index_tracker.h"
#include "logger.h"


std::unordered_map<std::string, std::string> CCT_PREFIX_2_INDEX;
std::set<std::string> TRACKED_INDEXES;
std::unordered_map<std::string, std::set<std::string>> CCT_TRACKED_INDEX_2_CLIENTS;

void OnRedisReady(RedisModuleCtx *ctx, RedisModuleEvent event, uint64_t subevent, void *data) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "OnRedisReady event : " + std::to_string(event.id) + " subevent : " + std::to_string(subevent) );
    if (event.id == REDISMODULE_EVENT_LOADING && subevent == REDISMODULE_SUBEVENT_LOADING_ENDED) {
        auto& index_manager = Redis_Index_Manager::Instance();
        std::set<std::string> indexes = index_manager.Get_All_Indexes(ctx);
        index_manager.Get_Index_Prefixes(ctx , indexes);
    }
}

void Redis_Index_Manager::Set_Index_Change(bool change){
    active_index_change = change;
}

bool Redis_Index_Manager::Get_Index_Change() const{
    return active_index_change;
}

std::set<std::string> Redis_Index_Manager::Get_All_Indexes(RedisModuleCtx *ctx) {
    RedisModule_AutoMemory(ctx);
    std::set<std::string> indexes;
    RedisModuleCallReply *ft_list_reply = RedisModule_Call(ctx, "FT._LIST", "");
    if (ft_list_reply == NULL || RedisModule_CallReplyType(ft_list_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_All_Indexes has failed with ft._list failed. Call failed." );
        return indexes;
    }

    if (RedisModule_CallReplyType(ft_list_reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_All_Indexes has failed with ft._list failed. Not array." );
        return indexes;
    }

    const size_t ft_list_length = RedisModule_CallReplyLength(ft_list_reply);
    for (size_t i = 0; i < ft_list_length; i++) {
        RedisModuleCallReply *index_reply = RedisModule_CallReplyArrayElement(ft_list_reply, i);
        if (RedisModule_CallReplyType(index_reply) == REDISMODULE_REPLY_STRING){ 
            RedisModuleString *index = RedisModule_CreateStringFromCallReply(index_reply);
            std::string index_str = RedisModule_StringPtrLen(index, NULL);
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_All_Indexes index is added : " + index_str );
            indexes.insert(index_str);
        }
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_All_Indexes index is succesfully finished" );
    return indexes;
}

void Redis_Index_Manager::Get_Index_Prefixes(RedisModuleCtx *ctx, std::set<std::string> indexes) {
    RedisModule_AutoMemory(ctx);
    for(const auto& index : indexes){
        RedisModuleCallReply *info_reply = RedisModule_Call(ctx, "FT.INFO", "c", index.c_str());
        if (info_reply == NULL || RedisModule_CallReplyType(info_reply) == REDISMODULE_REPLY_ERROR) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Index_Prefixes has failed with ft.info failed. Call failed." );
        }

        if (RedisModule_CallReplyType(info_reply) != REDISMODULE_REPLY_ARRAY) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Index_Prefixes has failed with ft.info failed. Not array." );
        }

        const int index_definition_index = 5;
        const int prefixes_index = 3; 
        RedisModuleCallReply *index_definition_reply = RedisModule_CallReplyArrayElement(info_reply, index_definition_index);
        if (RedisModule_CallReplyType(index_definition_reply) == REDISMODULE_REPLY_ARRAY && 
                RedisModule_CallReplyLength(index_definition_reply) > prefixes_index ) {
                RedisModuleCallReply *prefixes_reply = RedisModule_CallReplyArrayElement(index_definition_reply, prefixes_index);
                if (RedisModule_CallReplyType(prefixes_reply) == REDISMODULE_REPLY_ARRAY) {
                    const size_t prefixes_length = RedisModule_CallReplyLength(prefixes_reply);
                        for (size_t i = 0; i < prefixes_length; i++) {
                            RedisModuleCallReply *prefix_reply = RedisModule_CallReplyArrayElement(prefixes_reply, i);
                            if (RedisModule_CallReplyType(prefix_reply) == REDISMODULE_REPLY_STRING){
                                RedisModuleString *prefix_redis_str = RedisModule_CreateStringFromCallReply(prefix_reply);
                                std::string prefix = RedisModule_StringPtrLen(prefix_redis_str, NULL);
                                CCT_PREFIX_2_INDEX[prefix] = index; // multiple index using same prefix scenario is not covered
                                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Index_Prefixes prefix : " + prefix + " is added for index : "  + index);
                            }
                        }
                }
        }
              
    }
}

void Track_Index(std::string index, std::string client_tracking_group) {
    if(CCT_TRACKED_INDEX_2_CLIENTS.find(index) == CCT_TRACKED_INDEX_2_CLIENTS.end()){
        CCT_TRACKED_INDEX_2_CLIENTS[index] = std::set<std::string>{client_tracking_group};
    } else {
        CCT_TRACKED_INDEX_2_CLIENTS[index].insert(client_tracking_group);
    }
    TRACKED_INDEXES.insert(index);
}

const std::set<std::string> Get_Tracked_Index_Clients(std::string index){
    return CCT_TRACKED_INDEX_2_CLIENTS[index];
}

void UnTrack_Index(std::string index, std::string client_name) { 
    TRACKED_INDEXES.erase(index);
}

bool Is_Index_Tracked(std::string index){
    return TRACKED_INDEXES.find(index) != TRACKED_INDEXES.end();
}

std::string Get_Index_From_Prefix(std::string prefix) {
    if (CCT_PREFIX_2_INDEX.find(prefix) == CCT_PREFIX_2_INDEX.end()){
        return "";
    }
    return CCT_PREFIX_2_INDEX[prefix];
}

std::string Get_Tracked_Index_From_Prefix(std::string prefix) {
    std::string index = Get_Index_From_Prefix(prefix);
    if ( index == ""){
        return "";
    } 
    if( Is_Index_Tracked(index) ) {
        return index;
    }
    return "";
}

std::string Get_Tracked_Index_From_Key(std::string key) {
    for(const auto& kv: CCT_PREFIX_2_INDEX){
        if( key.compare(0, kv.first.size(), kv.first) == 0 ){
            return Get_Tracked_Index_From_Prefix(kv.first);
        }
    }
    return "";
}

std::string Get_Index_From_Key(std::string key) {
    for(const auto& kv: CCT_PREFIX_2_INDEX){
        if( key.compare(0, kv.first.size(), kv.first) == 0 ){
            return Get_Index_From_Prefix(kv.first);
        }
    }
    return "";    
}

void Start_Index_Change_Handler(RedisModuleCtx *ctx) {
    std::thread index_checker_thread(Index_Change_Handler, ctx);
    index_checker_thread.detach();    
}

void Index_Change_Handler(RedisModuleCtx *ctx) {
    auto& index_manager = Redis_Index_Manager::Instance();
    while(true) {
        if(index_manager.Get_Index_Change()) {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Index_Change_Handler index change scan started" );
            std::set<std::string> indexes = index_manager.Get_All_Indexes(ctx);
            index_manager.Get_Index_Prefixes(ctx , indexes);
            index_manager.Set_Index_Change(false);
        }   
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}