#pragma once

#include <string>
#include <unordered_map>

const std::string CCT_MODULE_PREFIX  = "CCT2:" ;
const std::string CCT_MODULE_KEY_2_CLIENT = CCT_MODULE_PREFIX + "K2C:" ;
const std::string CCT_MODULE_KEY_2_QUERY = CCT_MODULE_PREFIX + "K2Q:" ;
const std::string CCT_MODULE_QUERY_2_CLIENT = CCT_MODULE_PREFIX + "Q2C:" ;
const std::string CCT_MODULE_QUERY_2_KEY =  CCT_MODULE_PREFIX + "Q2K:" ;
const std::string CCT_MODULE_QUERY_CLIENT =  CCT_MODULE_PREFIX + "QC:" ;
const std::string CCT_MODULE_CLIENT_2_QUERY = CCT_MODULE_PREFIX + "C2Q:" ;
const std::string CCT_MODULE_KEY_OLD_VALUE = CCT_MODULE_PREFIX + "OLD:" ;
const std::string CCT_MODULE_CACHED_QUERY  = CCT_MODULE_PREFIX + "CQ:" ;
const std::string CCT_MODULE_KEY_SEPERATOR  = ":" ;
const std::string CCT_MODULE_KEY_LEVEL = "." ;
const std::string CCT_MODULE_KEY_LEVEL_WITH_ESCAPE = "\\." ;
const std::string CCT_MODULE_QUERY_DELIMETER = "-CCT_DEL-";
const std::string CCT_MODULE_END_OF_SNAPSHOT = "-END_OF_SNAPSHOT-";
const int CLIENT_OFFLINE = 0 ;

const std::string WILDCARD_SEARCH  = "*";

const std::string REDIS_JSON_SET_EVENT = "json.set";
const std::string REDIS_DEL_EVENT = "del";
const std::string REDIS_EXPIRED_EVENT = "expired";

const std::string CCT_UPDATE_EVENT = "UPDATE";
const std::string CCT_DELETE_EVENT = "DELETE";
const std::string CCT_EXPIRE_EVENT = "EXPIRE";
const std::string CCT_NEW_QUERY_EVENT = "new_query";

const std::unordered_map<std::string, std::string> CCT_KEY_EVENTS =
                                                                    {
                                                                        {REDIS_JSON_SET_EVENT, CCT_UPDATE_EVENT},
                                                                        {REDIS_DEL_EVENT, CCT_DELETE_EVENT},
                                                                        {REDIS_EXPIRED_EVENT, CCT_DELETE_EVENT},
                                                                        {"query_expired", CCT_EXPIRE_EVENT},
                                                                        {"query", CCT_NEW_QUERY_EVENT}
                                                                    } ;

const int MS_MULT = 1000 ;
#ifdef NDEBUG
const int CCT_QUERY_TTL_SECOND = 7 * 24 * 60 * 60  ; // 7 days
const int CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND = 2 * 60 ; // 2 Minutes
const int CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT = 3;
#else
const int CCT_QUERY_TTL_SECOND = 1  ; // 1 second
const int CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND = 1 ; // 1 second
const int CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT = 2;
#endif

const int CCT_SEND_OLD_VALUE = 0 ;
const int CCT_AGGREGATE_HANDLER = 1;
const int CCT_AGGREGATE_HANDLER_INTERVAL_SECOND = 1;

typedef struct  {
    int CCT_QUERY_TTL_SECOND_CFG = CCT_QUERY_TTL_SECOND;
    int CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CFG = CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND;
    int CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CFG = CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT;
    int CCT_SEND_OLD_VALUE_CFG = CCT_SEND_OLD_VALUE;
    int CCT_AGGREGATE_HANDLER_CFG = CCT_AGGREGATE_HANDLER;
    int CCT_AGGREGATE_HANDLER_INTERVAL_SECOND_CFG = CCT_AGGREGATE_HANDLER_INTERVAL_SECOND;
} CCT_Config;

extern CCT_Config cct_config;

const std::string CCT_OPERATION = "operation" ;
const std::string CCT_KEY = "key" ;
const std::string CCT_VALUE = "value" ;
const std::string CCT_OLD_VALUE = "old_value" ;
const std::string CCT_QUERIES = "queries" ;
