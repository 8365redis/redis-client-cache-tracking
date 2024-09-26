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

std::string Get_Query_Normalized(const RedisModuleString *query) {
    const std::string query_str = RedisModule_StringPtrLen(query, NULL);
    //printf("Query : %s \n", query_str.c_str());
    std::string query_term_attribute_normalized;
    const std::string q_term = Get_Query_Term(query_str);
    const std::string q_attribute = Get_Query_Attribute(query_str);
    query_term_attribute_normalized += q_term + CCT_MODULE_KEY_SEPERATOR + q_attribute;
    //printf("Query before Normalized : %s \n", query_term_attribute_normalized.c_str());
    //printf("Query Normalized : %s \n", query_term_attribute_normalized.c_str());
    return query_term_attribute_normalized;
}

std::string Normalized_to_Original(const std::string normalized_query) {
    if(normalized_query.find(CCT_MODULE_KEY_SEPERATOR) == std::string::npos) {
        return "";
    }
    const std::string query = normalized_query.substr(0, normalized_query.find(CCT_MODULE_KEY_SEPERATOR));
    const std::string attribute = normalized_query.substr(normalized_query.find(CCT_MODULE_KEY_SEPERATOR) + 1, normalized_query.length() - normalized_query.find(CCT_MODULE_KEY_SEPERATOR));
    const std::string original_query = TERM_START + query + CCT_MODULE_KEY_SEPERATOR + TAG_ATTRIBUTE_START + attribute + TAG_ATTRIBUTE_END ;
    return original_query;
}