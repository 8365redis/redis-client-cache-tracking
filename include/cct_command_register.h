#ifndef CCT_COMMAND_REGISTER_H
#define CCT_COMMAND_REGISTER_H
#include "redismodule.h"

int Register_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Send_Snapshot(RedisModuleCtx *ctx, RedisModuleKey *stream_key, std::string client);

#endif /* CCT_COMMAND_REGISTER_H */