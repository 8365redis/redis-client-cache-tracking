#ifndef LOGGER_H
#define LOGGER_H

#include "redismodule.h"
#include <string>
#include <cstring>
#include <chrono>


#ifdef _DEBUG
#define LOG(ctx, level, log) Log_Std_Output(ctx, level, log)
#else
#define LOG(ctx, level, log) Log_Redis(ctx, level, log)
#endif

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt );
void Log_Redis(RedisModuleCtx *ctx, const char *levelstr, std::string fmt );

#endif /* LOGGER_H */

