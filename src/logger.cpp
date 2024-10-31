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

// Implementation of Vector_To_String
template <typename T>
std::string Vector_To_String(const std::vector<T>& input_vector, const std::string& delimiter) {
    std::ostringstream output_stream;

    for (size_t i = 0; i < input_vector.size(); ++i) {
        output_stream << input_vector[i];
        if (i != input_vector.size() - 1) {
            output_stream << delimiter;
        }
    }

    return output_stream.str();
}

// Implementation of Set_To_String
template <typename T>
std::string Set_To_String(const std::set<T>& input_set, const std::string& delimiter) {
    std::ostringstream output_stream;

    for (auto it = input_set.begin(); it != input_set.end(); ++it) {
        output_stream << *it;
        if (std::next(it) != input_set.end()) {
            output_stream << delimiter;
        }
    }

    return output_stream.str();
}

// Explicit template instantiations to avoid linker errors
template std::string Vector_To_String<int>(const std::vector<int>&, const std::string&);
template std::string Vector_To_String<std::string>(const std::vector<std::string>&, const std::string&);
template std::string Set_To_String<int>(const std::set<int>&, const std::string&);
template std::string Set_To_String<std::string>(const std::set<std::string>&, const std::string&);



