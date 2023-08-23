#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include <string>

#define TERM_START "@"
#define TERM_END ":"

#define TAG_ATTRIBUTE_START "{"
#define TAG_ATTRIBUTE_END "}"

std::string Get_Str_Between(const std::string &s,
        const std::string &start_delim,
        const std::string &stop_delim);

std::string Get_Query_Term(const std::string &s);

std::string Get_Query_Attribute(const std::string &s);


#endif /* QUERY_PARSER_H */