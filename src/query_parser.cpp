#include "query_parser.h"

std::string Get_Str_Between(const std::string &s,
        const std::string &start_delim,
        const std::string &stop_delim) {
    unsigned first_delim_pos = s.find(start_delim);
    unsigned end_pos_of_first_delim = first_delim_pos + start_delim.length();
    unsigned last_delim_pos = s.find(stop_delim);
 
    return s.substr(end_pos_of_first_delim, last_delim_pos - end_pos_of_first_delim);
}

std::string Get_Query_Term(const std::string &s) {
    return Get_Str_Between(s, TERM_START, TERM_END);
}

std::string Get_Query_Attribute(const std::string &s) {
    return Get_Str_Between(s, TAG_ATTRIBUTE_START, TAG_ATTRIBUTE_END);
}

std::string Normalized_to_Original(const std::string normalized_query) {
    if(normalized_query.find(CCT_MODULE_KEY_SEPERATOR) == std::string::npos) {
        return "";
    }
    std::string query = normalized_query.substr(0, normalized_query.find(CCT_MODULE_KEY_SEPERATOR));
    std::string attribute = normalized_query.substr(normalized_query.find(CCT_MODULE_KEY_SEPERATOR) + 1, normalized_query.length() - normalized_query.find(CCT_MODULE_KEY_SEPERATOR));
    std::string original_query = TERM_START + query + CCT_MODULE_KEY_SEPERATOR + TAG_ATTRIBUTE_START + attribute + TAG_ATTRIBUTE_END ;
    return original_query;
}