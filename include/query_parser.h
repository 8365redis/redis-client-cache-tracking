#pragma once

#include <string>
#include <vector>
#include <set>
#include "redismodule.h"
#include "constants.h"

const std::string TERM_START = "@";
const std::string TERM_END = ":";

const std::string TAG_ATTRIBUTE_START = "{";
const std::string TAG_ATTRIBUTE_END = "}";

std::string Get_Str_Between(const std::string &s,
        const std::string &start_delim,
        const std::string &stop_delim);
std::string Get_Query_Term(const std::string &s);
std::string Get_Query_Attribute(const std::string &s);
std::string Normalized_to_Original(const std::string normalized_query);
std::string Normalized_to_Original_With_Index(const std::string normalized_query_with_index);
std::string Get_Query_Normalized(const RedisModuleString *query);
std::string Escape_Special_Chars(const std::string &input);
std::string Escape_FtQuery(const std::string &input);

std::string Concate_Queries(std::vector<std::string> queries);
std::string Concate_Queries(std::set<std::string> queries);

void FindAndRemoveClientName(RedisModuleString **argv, int *argc, RedisModuleString **clientname);
