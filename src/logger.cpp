#include "logger.h"
#include <time.h>

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    printf("XXXXX:X %d-%02d-%02d %02d:%02d:%02d.00 * <CCT_MODULE> %s\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fmt.c_str());
}

void Log_Redis(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    if( strcmp(levelstr, REDISMODULE_LOGLEVEL_WARNING) != 0 ) {
        return;
    }
    RedisModule_Log(ctx, levelstr, "%s", fmt.c_str());
}

void Log_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

}