#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include <string>
#include "constants.h"

#define TERM_START "@"
#define TERM_END ":"

#define TAG_ATTRIBUTE_START "{"
#define TAG_ATTRIBUTE_END "}"

std::string Get_Str_Between(const std::string &s,
        const std::string &start_delim,
        const std::string &stop_delim);
std::string Get_Query_Term(const std::string &s);
std::string Get_Query_Attribute(const std::string &s);
std::string Normalized_to_Original(const std::string normalized_query);

#endif /* QUERY_PARSER_H */