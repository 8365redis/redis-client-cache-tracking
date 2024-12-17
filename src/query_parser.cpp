#include "query_parser.h"

#include <cstring>
#include <unordered_map>

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
    //std::string query_term_attribute_normalized;
    const std::string q_term = Get_Query_Term(query_str);
    const std::string q_attribute = Get_Query_Attribute(query_str);
    std::string query_term_attribute_normalized = q_term + CCT_MODULE_KEY_SEPERATOR + q_attribute;
    //printf("Query before Normalized : %s \n", query_term_attribute_normalized.c_str());
    //printf("Query Normalized : %s \n", query_term_attribute_normalized.c_str());
    return query_term_attribute_normalized;
}

std::string Normalized_to_Original_With_Index(const std::string normalized_query_with_index) {
    std::string index = normalized_query_with_index.substr(0, normalized_query_with_index.find(CCT_MODULE_KEY_SEPERATOR));
    std::string normalized_query = normalized_query_with_index.substr(normalized_query_with_index.find(CCT_MODULE_KEY_SEPERATOR) + 1, normalized_query_with_index.length() - normalized_query_with_index.find(CCT_MODULE_KEY_SEPERATOR));
    return index + CCT_MODULE_KEY_SEPERATOR + Normalized_to_Original(normalized_query);

}

std::string Normalized_to_Original(const std::string normalized_query) {
    if(normalized_query.find(CCT_MODULE_KEY_SEPERATOR) == std::string::npos) {
        return normalized_query;
    }
    const std::string query = normalized_query.substr(0, normalized_query.find(CCT_MODULE_KEY_SEPERATOR));
    const std::string attribute = normalized_query.substr(normalized_query.find(CCT_MODULE_KEY_SEPERATOR) + 1, normalized_query.length() - normalized_query.find(CCT_MODULE_KEY_SEPERATOR));
    const std::string original_query = TERM_START + query + CCT_MODULE_KEY_SEPERATOR + TAG_ATTRIBUTE_START + attribute + TAG_ATTRIBUTE_END ;
    return original_query;
}

std::unordered_map<char, std::string> specialChars = {
    {',', "\\,"}, {'.', "\\."}, {'<', "\\<"}, {'>', "\\>"},
    {'{', "\\{"}, {'}', "\\}"}, {'[', "\\["}, {'"', "\\\""},
    {'\\', "\\\\"}, {':', "\\:"}, {';', "\\;"}, {']', "\\]"},
    {'!', "\\!"}, {'@', "\\@"}, {'#', "\\#"}, {'$', "\\$"},
    {'%', "\\%"}, {'^', "\\^"}, {'*', "\\*"}, {'(', "\\("},
    {'-', "\\-"}, {'+', "\\+"}, {'=', "\\="}, {'~', "\\~"},
    {'/', "\\/"}
};

std::string Escape_Special_Chars(const std::string &input) {
    std::string escapedString;
    escapedString.reserve(input.length() * 2);

    bool isEscaped = false;

    for (size_t i = 0; i < input.length(); ++i) {
        char c = input[i];

        // If the previous character was an escape character, we don't want to double escape
        if (isEscaped) {
            escapedString.push_back(c);
            isEscaped = false;
            continue;
        }

        if (c == '\\') {
            escapedString.push_back(c);
            isEscaped = true;
            continue;
        }

        if (specialChars.find(c) != specialChars.end()) {
            escapedString.append(specialChars[c]);
        } else {
            escapedString.push_back(c);
        }
    }

    return escapedString;
}

std::string Escape_FtQuery(const std::string &input) {
    size_t colonPos = input.find(':');
    if (colonPos == std::string::npos) {
        // No colon found, return the input as is
        return input;
    }

    std::string beforeColon = input.substr(0, colonPos + 1);
    std::string afterColon = input.substr(colonPos + 1);

    std::string escapedAfterColon = Escape_Special_Chars(afterColon);

    return beforeColon + escapedAfterColon;
}

template <typename Container>
std::string Concate_Queries_Helper(const Container& queries) {
    std::string client_queries_str;
    for (const auto& e : queries) {
        client_queries_str += (e + CCT_MODULE_QUERY_DELIMETER);
    }
    if (client_queries_str.length() > CCT_MODULE_QUERY_DELIMETER.length()) {
        client_queries_str.erase(client_queries_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
    }
    return client_queries_str;
}

// Original function for std::vector
std::string Concate_Queries(std::vector<std::string> queries) {
    return Concate_Queries_Helper(queries);
}

// Overloaded function for std::set
std::string Concate_Queries(std::set<std::string> queries) {
    return Concate_Queries_Helper(queries);
}

void FindAndRemoveClientName(RedisModuleString **argv, int *argc, RedisModuleString **clientname) {
    // Ensure we have at least 2 arguments to look for "CLIENTNAME" and its value.
    if (*argc < 2) {
        return;
    }

    // Iterate over argv to find "CLIENTNAME"
    for (int i = 0; i < *argc; i++) {
        size_t len;
        const char *arg_str = RedisModule_StringPtrLen(argv[i], &len);

        // Check if current argument is "CLIENTNAME"
        if (std::strcmp(arg_str, CCT_CLIENTNAME.c_str()) == 0) {
            if (i + 1 >= *argc) {
                return; // CLIENTNAME provided without a value
            }

            // Set clientname to the next argument
            *clientname = argv[i + 1];

            // Remove "CLIENTNAME" and its value from argv and argc
            for (int j = i; j < *argc - 2; j++) {
                argv[j] = argv[j + 2];
            }

            *argc -= 2;
            return;
        }
    }

    // If "CLIENTNAME" wasn't found, set clientname to NULL
    *clientname = NULL;
    return ;
}
