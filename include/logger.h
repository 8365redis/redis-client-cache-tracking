#pragma once

#include "redismodule.h"
#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <sstream>

#ifdef _DEBUG
#define LOG(ctx, level, log) Log_Std_Output(ctx, level, log)
#else
#define LOG(ctx, level, log) Log_Redis(ctx, level, log)
#endif

void Log_Std_Output(RedisModuleCtx *ctx, const char *levelstr, std::string fmt );
void Log_Redis(RedisModuleCtx *ctx, const char *levelstr, std::string fmt );

// Helpers
// Function to convert vector to string
template <typename T>
std::string Vector_To_String(const std::vector<T>& input_vector, const std::string& delimiter = ", ");

// Function to convert set to string
template <typename T>
std::string Set_To_String(const std::set<T>& input_set, const std::string& delimiter = ", ");

