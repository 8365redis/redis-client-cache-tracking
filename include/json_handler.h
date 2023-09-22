#ifndef JSON_HANDLER_H
#define JSON_HANDLER_H

#include "redismodule.h"
#include "json.hpp"
#include "constants.h"
#include <string>
#include <vector>

using json = nlohmann::json;

RedisModuleString * Get_JSON_Value(RedisModuleCtx *ctx , std::string event_str, RedisModuleString* r_key);
RedisModuleString * Get_JSON_Value(RedisModuleCtx *ctx, std::string key_str);
json Get_JSON_Object(RedisModuleCtx *ctx, std::string str);
void Recursive_JSON_Iterate(const json& j, std::string prefix , std::vector<std::string> &keys);
std::string Get_Json_Str(RedisModuleCtx *ctx, std::string key_str);

#endif /* JSON_HANDLER_H */