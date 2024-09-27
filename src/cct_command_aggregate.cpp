#include <unordered_map>
#include <set>
#include <vector>
#include <thread>
#include "cct_command_aggregate.h"
#include "logger.h"
#include "client_tracker.h"

std::unordered_map<std::size_t, RedisModuleCallReply*> CCT_AGGREGATE_CACHED_QUERIES;
std::unordered_map<std::size_t, std::vector<RedisModuleString*>> CCT_AGGREGATE_CACHED_QUERIES_STR;
std::unordered_map<std::size_t, unsigned long long> CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE;
std::unordered_map<std::size_t, unsigned long long> CCT_AGGREGATE_CACHED_QUERIES_TTL;

static bool is_handler_running = false;

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
    std::size_t hashed_query = std::hash<std::string>{}(Query_2_String(query));
    CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE[hashed_query] = Get_Current_Ms(); // Update the Query time
    if(ttl > CCT_AGGREGATE_CACHED_QUERIES_TTL[hashed_query]) { // Bigger TTL is priority
        CCT_AGGREGATE_CACHED_QUERIES_TTL[hashed_query] = ttl;
    }
    return CCT_AGGREGATE_CACHED_QUERIES.count(hashed_query) > 0;
}

bool Is_Query_Expired(RedisModuleCtx *ctx, std::size_t query) {
    if (CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE.count(query) == 0 || CCT_AGGREGATE_CACHED_QUERIES_TTL.count(query) == 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Is_Query_Expired query is expired : " + std::to_string(query));
        return true;
    }
    unsigned long long query_time = CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE[query];
    unsigned long long query_ttl = CCT_AGGREGATE_CACHED_QUERIES_TTL[query];
    unsigned long long current = Get_Current_Ms();

    if ( (current -  query_time) > query_ttl) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Is_Query_Expired query is expired : " + std::to_string(query));
        return true;
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Is_Query_Expired query is not expired : " + std::to_string(query));
    return false;
}

void Cache_Query(RedisModuleCtx *ctx, std::vector<RedisModuleString*> query, RedisModuleCallReply *reply, unsigned long long ttl) {
    if (is_handler_running) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Cache_Query handler is running skipping caching operation for query: " + Query_2_String(query));
        return;
    }
    std::size_t hashed_query = std::hash<std::string>{}(Query_2_String(query));
    CCT_AGGREGATE_CACHED_QUERIES[hashed_query] = reply;
    for (std::size_t i=0; i< query.size(); i++){
        CCT_AGGREGATE_CACHED_QUERIES_STR[hashed_query].push_back(query[i]);
    }
    CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE[hashed_query] = Get_Current_Ms(); // Update the Query last usage in first usage
    CCT_AGGREGATE_CACHED_QUERIES_TTL[hashed_query] = ttl;

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Cache_Query caching the query reply: " + Query_2_String(query) + " with hash : " + std::to_string(hashed_query));
}

void Update_Cache(RedisModuleCtx *ctx, std::size_t hashed_query, RedisModuleCallReply *reply) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Update_Cache updating the query hash: " + std::to_string(hashed_query));
    CCT_AGGREGATE_CACHED_QUERIES[hashed_query] = reply;
}

RedisModuleCallReply* Get_Cache_Reply(std::vector<RedisModuleString*> &query){
    std::size_t hashed_query = std::hash<std::string>{}(Query_2_String(query));
    return CCT_AGGREGATE_CACHED_QUERIES[hashed_query];
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

void Aggregate_Handler(RedisModuleCtx *ctx, std::unordered_map<std::size_t, RedisModuleCallReply*> &cached_queries, std::unordered_map<std::size_t, unsigned long long> &cached_query_last_usages,
                         std::unordered_map<std::size_t, unsigned long long> &cached_query_ttls, std::unordered_map<std::size_t, std::vector<RedisModuleString*>> &cached_queries_str ) {
    while(true) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Aggregate_Handler started");
        is_handler_running = true;
        //Expire Queries
        std::vector<std::size_t> queries_2_expire;
        for(const auto &c_q : cached_queries){
            auto query = c_q.first;
            if(Is_Query_Expired(ctx, query)){
                queries_2_expire.push_back(query);
            }
        }
        for(const auto &q_2_e : queries_2_expire){
            cached_queries.erase(q_2_e);
            cached_query_last_usages.erase(q_2_e);
            cached_query_ttls.erase(q_2_e);
            cached_queries_str.erase(q_2_e);
            //TODO release reply first ??
        }

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
        is_handler_running = false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }    
}

void Start_Aggregate_Handler(RedisModuleCtx *ctx) {
    std::thread aggregate_handler_thread(Aggregate_Handler, ctx, std::ref(CCT_AGGREGATE_CACHED_QUERIES), std::ref(CCT_AGGREGATE_CACHED_QUERIES_LAST_USAGE), std::ref(CCT_AGGREGATE_CACHED_QUERIES_TTL), std::ref(CCT_AGGREGATE_CACHED_QUERIES_STR));
    aggregate_handler_thread.detach();
}



