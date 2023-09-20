#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <string>
#include <unordered_map>

const std::string CCT_MODULE_PREFIX  = "CCT:" ;
const std::string CCT_MODULE_KEY_2_CLIENT = CCT_MODULE_PREFIX + "K2C:" ;
const std::string CCT_MODULE_KEY_2_QUERY = CCT_MODULE_PREFIX + "K2Q:" ;
const std::string CCT_MODULE_QUERY_2_CLIENT = CCT_MODULE_PREFIX + "Q2C:" ;
const std::string CCT_MODULE_QUERY_2_KEY =  CCT_MODULE_PREFIX + "Q2K:" ;
const std::string CCT_MODULE_QUERY_CLIENT =  CCT_MODULE_PREFIX + "QC:" ;
const std::string CCT_MODULE_CLIENT_2_QUERY = CCT_MODULE_PREFIX + "C2Q:" ;
const std::string CCT_MODULE_KEY_SEPERATOR  = ":" ;
const std::string CCT_MODULE_KEY_LEVEL = "." ;
const std::string CCT_MODULE_KEY_LEVEL_WITH_ESCAPE = "\\." ;
const int CLIENT_OFFLINE = 0 ;

const std::unordered_map<std::string, std::string> CCT_KEY_EVENTS = 
                                                                    {
                                                                        {"json.set", "UPDATE"},
                                                                        {"del", "DELETE"},
                                                                        {"expired", "DELETE"},
                                                                        {"query_expired", "EXPIRE"}
                                                                    } ;

const int MS_MULT = 1000 ;
const int CCT_TTL_SECOND = 4 ; 
const int CCT_TTL = CCT_TTL_SECOND * MS_MULT ; 

#endif /* CONSTANTS_H */