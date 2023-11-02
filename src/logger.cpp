#include "logger.h"
#include <time.h>

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    printf("XXXXX:X %d-%02d-%02d %02d:%02d:%02d * <CCT_MODULE> %s\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fmt.c_str());
}

void Log_Redis(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    if( strcmp(levelstr, REDISMODULE_LOGLEVEL_WARNING) != 0 ) {
        return;
    }
    RedisModule_Log(ctx, levelstr, "%s", fmt.c_str());
}

void Log_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    std::stringstream argument_stream;
    std::string command_name = RedisModule_StringPtrLen(argv[0], NULL);
    for ( int i = 1; i < argc; i++) {
        argument_stream<<RedisModule_StringPtrLen(argv[i], NULL)<< " ";
    }
    #ifdef _DEBUG
    Log_Std_Output(ctx, REDISMODULE_LOGLEVEL_DEBUG , command_name + " command called with arguments " + argument_stream.str());
    #else
    Log_Redis(ctx, REDISMODULE_LOGLEVEL_DEBUG , command_name + " command called with arguments " + argument_stream.str());
    #endif
}