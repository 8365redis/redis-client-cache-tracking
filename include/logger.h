#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <string.h>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <iostream>

#include "redismodule.h"


#ifdef _DEBUG
#define LOG(ctx, level, log) Log_Std_Output(ctx, level, log)
#else
#define LOG(ctx, level, log) Log_Redis(ctx, level, log)
#endif

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt );
void Log_Redis(RedisModuleCtx *ctx, const char *levelstr, std::string fmt );
void Log_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* LOGGER_H */

