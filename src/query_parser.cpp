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
    std::string query_str = RedisModule_StringPtrLen(query, NULL);
    //printf("Query : %s \n", query_str.c_str());
    std::vector<std::string> query_split = Split_Query(query_str, ' ');
    std::string query_term_attribute_normalized = "";
    for(auto &q : query_split) {
        //printf("Query item : %s \n", q.c_str());
        std::string q_term = Get_Query_Term(q);
        std::string q_attribute = Get_Query_Attribute(q);
        query_term_attribute_normalized += q_term + CCT_MODULE_KEY_SEPERATOR + q_attribute + CCT_MODULE_QUERY_AND;
    }
    //printf("Query before Normalized : %s \n", query_term_attribute_normalized.c_str());
    if(query_term_attribute_normalized.length() > CCT_MODULE_QUERY_AND.length()){ 
        query_term_attribute_normalized.erase(query_term_attribute_normalized.length() - CCT_MODULE_QUERY_AND.length());
    }
    //printf("Query Normalized : %s \n", query_term_attribute_normalized.c_str());
    return query_term_attribute_normalized;
}

std::vector<std::string> Split_Query(const std::string &text, char sep) {
    std::vector<std::string> tokens;
    std::size_t start = 0, end = 0;
    while ((end = text.find(sep, start)) != std::string::npos) {
        tokens.push_back(text.substr(start, end - start));
        start = end + 1;
    }
    tokens.push_back(text.substr(start));
    return tokens;
}


std::string Normalized_to_Original(const std::string normalized_query) {
    if(normalized_query.find(CCT_MODULE_KEY_SEPERATOR) == std::string::npos) {
        return "";
    }

    std::string delimiter = CCT_MODULE_QUERY_AND;

    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    std::string token;
    std::vector<std::string> normalized_query_vec;
    while ((pos_end = normalized_query.find(delimiter, pos_start)) != std::string::npos) {
        token = normalized_query.substr (pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        normalized_query_vec.push_back (token);
    }
    normalized_query_vec.push_back (normalized_query.substr (pos_start));

    std::string original_query;
    for(auto &q : normalized_query_vec) {
        std::string query = q.substr(0, q.find(CCT_MODULE_KEY_SEPERATOR));
        std::string attribute = q.substr(q.find(CCT_MODULE_KEY_SEPERATOR) + 1, q.length() - q.find(CCT_MODULE_KEY_SEPERATOR));
        std::string one_query = TERM_START + query + CCT_MODULE_KEY_SEPERATOR + TAG_ATTRIBUTE_START + attribute + TAG_ATTRIBUTE_END ;
        original_query += one_query + " ";
    }
    if(original_query.length() > 1){ 
        original_query.erase(original_query.length() - 1);
    }
    return original_query;
}

std::set<std::string> Query_Permutations(std::vector<std::string> &queries) {
    std::set<std::string> permutations;
    do {
        for (size_t size = 1 ; size <= queries.size() ; size++){
            std::string permutation_query_str = "";
            for (size_t index = 0; index < size; index++) {
                permutation_query_str += queries[index] + CCT_MODULE_QUERY_AND;
            }
            if(permutation_query_str.length() > CCT_MODULE_QUERY_AND.length()){ 
                permutation_query_str.erase(permutation_query_str.length() - CCT_MODULE_QUERY_AND.length());
            }
            permutations.insert(permutation_query_str);
        }
    } while (std::next_permutation(queries.begin(), queries.end()));
    /*for (auto q : permutations) {
        printf("Permutation query : %s\n" , q.c_str() );
    }*/
    return permutations;
}