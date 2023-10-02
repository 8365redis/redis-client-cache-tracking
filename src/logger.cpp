#include "logger.h"

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) {
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch() % std::chrono::seconds{1});
    auto time = std::put_time(&tm, "%d %b %Y %H:%M:%S");
    std::cout<<"XXXXX:X "<<time<<"."<<std::to_string(ms.count())<<" * <CCT_MODULE> "<< fmt << std::endl;
}

void Log_Redis(RedisModuleCtx *ctx, const char *levelstr, std::string fmt ) { 
    if( strcmp(levelstr, REDISMODULE_LOGLEVEL_WARNING) != 0 ) {
        return;
    }
    RedisModule_Log(ctx, levelstr , fmt.c_str());
}

void Log_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    std::stringstream argument_stream;
    std::string command_name = RedisModule_StringPtrLen(argv[0], NULL);
    for ( int i = 1; i < argc; i++) {
        argument_stream<<RedisModule_StringPtrLen(argv[i], NULL)<< " ";
    }
    Log_Std_Output(ctx, REDISMODULE_LOGLEVEL_DEBUG , command_name + " command called with arguments " + argument_stream.str());
}