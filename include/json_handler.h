#pragma once

#include "redismodule.h"
#include "json/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

RedisModuleString * Get_JSON_Value(RedisModuleCtx *ctx , std::string event_str, RedisModuleString* r_key);
RedisModuleString * Get_JSON_Value(RedisModuleCtx *ctx, std::string key_str);
json Get_JSON_Object(RedisModuleCtx *ctx, std::string str);
void Recursive_JSON_Iterate(const json& j, std::string prefix , std::vector<std::string> &keys);
std::string Get_Json_Str(RedisModuleCtx *ctx, std::string key_str);

