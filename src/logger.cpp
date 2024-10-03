#include <sys/time.h>
#include <cstring>
#include <chrono>
#include "logger.h"

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    //time_t t = time(NULL);
    //struct tm tm = *localtime(&t);
    
    //printf("XXXXX:X %d-%02d-%02d %02d:%02d:%02d.00 * <CCT_MODULE> %s\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fmt.c_str());
    struct timeval tv;
    gettimeofday(&tv, NULL);
    time_t t = tv.tv_sec;
    struct tm tm = *localtime(&t);
    // Extracting milliseconds from the microseconds part
    int milliseconds = tv.tv_usec / 1000;
    printf("XXXXX:X %d-%02d-%02d %02d:%02d:%02d.%03d * <CCT_MODULE> %s\n", 
           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, 
           tm.tm_hour, tm.tm_min, tm.tm_sec, 
           milliseconds, fmt.c_str());

}

void Log_Redis(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    if( strcmp(levelstr, REDISMODULE_LOGLEVEL_WARNING) != 0 ) {
        return;
    }
    RedisModule_Log(ctx, levelstr, "%s", fmt.c_str());
}