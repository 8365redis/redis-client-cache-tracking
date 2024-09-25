#include <unordered_map>
#include <set>
#include <vector>
#include "cct_command_aggregate.h"
#include "logger.h"

std::unordered_map<std::size_t, RedisModuleCallReply*> CCT_AGGREGATE_CACHED_QUERIES;

std::string Query_2_String(std::vector<RedisModuleString*> &query){
    std::string arg_str = "";
    for(const auto &arg: query) {
        arg_str += RedisModule_StringPtrLen(arg, NULL);
    }
    return arg_str;
}

bool Is_Query_Cached(std::vector<RedisModuleString*> &query) {
    std::size_t hashed_query = std::hash<std::string>{}(Query_2_String(query));
    return CCT_AGGREGATE_CACHED_QUERIES.count(hashed_query) > 0;
}

void Cache_Query(RedisModuleCtx *ctx, std::vector<RedisModuleString*> &query, RedisModuleCallReply *reply) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Cache_Query caching for query : " + Query_2_String(query));
    std::size_t hashed_query = std::hash<std::string>{}(Query_2_String(query));
    CCT_AGGREGATE_CACHED_QUERIES[hashed_query] = reply;
}

RedisModuleCallReply* Get_Cache_Reply(std::vector<RedisModuleString*> &query){
    std::size_t hashed_query = std::hash<std::string>{}(Query_2_String(query));
    return CCT_AGGREGATE_CACHED_QUERIES[hashed_query];
} 

int Aggregate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    std::vector<RedisModuleString*> query(argv+1, argv + argc);

    if(Is_Query_Cached(query)) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Aggregate_RedisCommand returning cached result for query : " + Query_2_String(query));
        return RedisModule_ReplyWithCallReply(ctx, Get_Cache_Reply(query));
    }

    // Forward Search
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

    Cache_Query(ctx, query, reply);

    return RedisModule_ReplyWithCallReply(ctx, reply);
}