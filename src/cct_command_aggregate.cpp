#include <unordered_map>
#include <set>
#include <vector>
#include <thread>
#include <atomic>
#include "cct_command_aggregate.h"
#include "logger.h"
#include "client_tracker.h"
#include "constants.h"

std::unordered_map<std::string, RedisModuleCallReply*> CCT_AGGREGATE_CACHED_QUERIES;
std::unordered_map<std::string, std::vector<RedisModuleString*>> CCT_AGGREGATE_CACHED_QUERIES_STR;
std::unordered_map<std::string, unsigned long long> CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE;
std::unordered_map<std::string, unsigned long long> CCT_AGGREGATE_CACHED_QUERIES_TTL;

std::atomic<bool> is_handler_running(false);
std::atomic<bool> invalidate_all_cache(false);

unsigned long long Get_Current_Ms(){
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    return ms.count();
}

std::string Query_2_String(std::vector<RedisModuleString*> &query){
    std::string arg_str = "";
    for(const auto &arg: query) {
        arg_str += RedisModule_StringPtrLen(arg, NULL);
    }
    return arg_str;
}

bool Is_Query_Cached(std::vector<RedisModuleString*> &query, unsigned long long ttl) {
    std::string query_str = Query_2_String(query);
    CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE[query_str] = Get_Current_Ms(); // Update the Query time
    if(ttl > CCT_AGGREGATE_CACHED_QUERIES_TTL[query_str]) { // Bigger TTL is priority
        CCT_AGGREGATE_CACHED_QUERIES_TTL[query_str] = ttl;
    }
    return CCT_AGGREGATE_CACHED_QUERIES.count(query_str) > 0;
}

bool Is_Query_Expired(RedisModuleCtx *ctx, std::string query_str) {
    if (CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE.count(query_str) == 0 || CCT_AGGREGATE_CACHED_QUERIES_TTL.count(query_str) == 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Is_Query_Expired query is expired : " + query_str);
        return true;
    }
    unsigned long long query_time = CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE[query_str];
    unsigned long long query_ttl = CCT_AGGREGATE_CACHED_QUERIES_TTL[query_str];
    unsigned long long current = Get_Current_Ms();

    if ( (current -  query_time) > query_ttl) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Is_Query_Expired query is expired : " + query_str);
        return true;
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Is_Query_Expired query is not expired : " + query_str);
    return false;
}

void Cache_Query(RedisModuleCtx *ctx, std::vector<RedisModuleString*> query, RedisModuleCallReply *reply, unsigned long long ttl) {
    if (is_handler_running) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Cache_Query handler is running skipping caching operation for query: " + Query_2_String(query));
        return;
    }
    std::string query_str = Query_2_String(query);
    CCT_AGGREGATE_CACHED_QUERIES[query_str] = reply;
    for (std::size_t i=0; i< query.size(); i++){
        CCT_AGGREGATE_CACHED_QUERIES_STR[query_str].push_back(query[i]);
    }
    CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE[query_str] = Get_Current_Ms(); // Update the Query last usage in first usage
    CCT_AGGREGATE_CACHED_QUERIES_TTL[query_str] = ttl;

    RedisModuleCallReply *sadd_reply_key = RedisModule_Call(ctx, "SADD", "cc", CCT_MODULE_CACHED_QUERIES.c_str()  , query_str.c_str());
    if (RedisModule_CallReplyType(sadd_reply_key) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Cache_Query failed while saving the cached query to CCT_MODULE_CACHED_QUERIES : " +  query_str);
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Cache_Query caching the query : " + query_str);
}

void Update_Cache(RedisModuleCtx *ctx, std::string query_str, RedisModuleCallReply *reply) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Update_Cache updating the query : " + query_str);
    CCT_AGGREGATE_CACHED_QUERIES[query_str] = reply;
}

RedisModuleCallReply* Get_Cache_Reply(std::vector<RedisModuleString*> &query){
    std::string query_str = Query_2_String(query);
    return CCT_AGGREGATE_CACHED_QUERIES[query_str];
}

int Invalidate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Invalidate_RedisCommand called");
    invalidate_all_cache = true;
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int Aggregate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    std::vector<RedisModuleString*> query;
    for (int i = 1; i < argc; i++) {
        size_t len;
        const char *str = RedisModule_StringPtrLen(argv[i], &len);
        RedisModuleString *new_str = RedisModule_CreateString(ctx, str, len);
        query.push_back(new_str);
    }

    std::string client_name_str = Get_Client_Name(ctx);
    std::string client_tracking_group = Get_Client_Client_Tracking_Group(client_name_str);
    unsigned long long client_ttl = Get_Client_Query_TTL(client_tracking_group);

    if(Is_Query_Cached(query, client_ttl)) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Aggregate_RedisCommand returning cached result for query : " + Query_2_String(query));
        return RedisModule_ReplyWithCallReply(ctx, Get_Cache_Reply(query));
    }

    // Forward Search
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Aggregate_RedisCommand forwarding request : " + Query_2_String(query));
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.AGGREGATE", "Ev", argv + 1, argc - 1);
    
    if (reply == nullptr) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Aggregate_RedisCommand failed : Returned null");
        return RedisModule_ReplyWithError(ctx, "Aggregate_RedisCommand returned null");
    }

    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        std::string error_msg = RedisModule_CallReplyStringPtr(reply, NULL);
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Aggregate_RedisCommand failed : " + error_msg);
        RedisModule_FreeCallReply(reply);
        return RedisModule_ReplyWithError(ctx, error_msg.c_str());
    }

    Cache_Query(ctx, query, reply, client_ttl);
    return RedisModule_ReplyWithCallReply(ctx, reply);
}

void Aggregate_Handler(RedisModuleCtx *ctx, std::unordered_map<std::string, RedisModuleCallReply*> &cached_queries, std::unordered_map<std::string, unsigned long long> &cached_query_last_usages,
                         std::unordered_map<std::string, unsigned long long> &cached_query_ttls, std::unordered_map<std::string, std::vector<RedisModuleString*>> &cached_queries_str) {
    while(true) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Aggregate_Handler started");
        is_handler_running = true;

        
        //Expire Queries
        std::vector<std::string> queries_2_expire;
        for(const auto &c_q : cached_queries){
            auto query = c_q.first;
            if(Is_Query_Expired(ctx, query) || invalidate_all_cache){
                queries_2_expire.push_back(query);
            }
        }
        for(const auto &q_2_e : queries_2_expire){
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Aggregate_Handler expiring query : " + q_2_e);
            // Delete query reply
            RedisModule_FreeCallReply(cached_queries[q_2_e]);
            cached_queries.erase(q_2_e);
            // Delete TTL and Last Usage
            cached_query_last_usages.erase(q_2_e);
            cached_query_ttls.erase(q_2_e);
            // Delete query strings
            for(auto str : cached_queries_str[q_2_e]){
                RedisModule_FreeString(ctx, str);
            }
            cached_queries_str.erase(q_2_e);
        }
        invalidate_all_cache = false;

        RedisModule_ThreadSafeContextLock(ctx);
        // Now delete the metadata of the not cached anymore queries
        for(const auto &q_2_e : queries_2_expire) {
            RedisModuleCallReply *srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", CCT_MODULE_CACHED_QUERIES.c_str()  , q_2_e.c_str());
            if (RedisModule_CallReplyType(srem_key_reply) != REDISMODULE_REPLY_INTEGER ){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Aggregate_Handler failed while deleting cached query from metadata: " +  q_2_e);
            }            
        }

        if(cct_config.CCT_AGGREGATE_HANDLER_CFG != 0){
            // Now run the cached queries and update the cache
            for(const auto &c_q : cached_queries){
                std::vector<RedisModuleString*> query = cached_queries_str[c_q.first];
                // Forward Search
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Aggregate_Handler forwarding request : " + Query_2_String(query));
                RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.AGGREGATE", "Ev", query.begin(), query.size());
                if (reply == nullptr) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Aggregate_Handler failed : Returned null");
                    continue;
                }
                if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
                    std::string error_msg = RedisModule_CallReplyStringPtr(reply, NULL);
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING, "Aggregate_Handler failed : " + error_msg);
                    RedisModule_FreeCallReply(reply);
                    continue;
                }
                Update_Cache(ctx, c_q.first, reply);
            }
        }
        RedisModule_ThreadSafeContextUnlock(ctx);

        is_handler_running = false;
        
        std::this_thread::sleep_for(std::chrono::milliseconds(cct_config.CCT_AGGREGATE_HANDLER_INTERVAL_SECOND_CFG * MS_MULT));
    }
}

void Start_Aggregate_Handler(RedisModuleCtx *ctx) {
    std::thread aggregate_handler_thread(Aggregate_Handler, ctx, std::ref(CCT_AGGREGATE_CACHED_QUERIES), std::ref(CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE), std::ref(CCT_AGGREGATE_CACHED_QUERIES_TTL), std::ref(CCT_AGGREGATE_CACHED_QUERIES_STR));
    aggregate_handler_thread.detach();
}



