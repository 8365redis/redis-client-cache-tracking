#ifndef CCT_OFFLINE_QUERY_EXPIRY_H
#define CCT_OFFLINE_QUERY_EXPIRY_H

#include "redismodule.h"

void Handle_RDB_Event(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
int Handle_Offline_Query_Expire(RedisModuleCtx *ctx);

#endif /* CCT_OFFLINE_QUERY_EXPIRY_H */