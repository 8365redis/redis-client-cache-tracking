#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <string>
#include <unordered_map>

const std::string CCT_MODULE_PREFIX  = "CCT:" ;
const std::string CCT_MODULE_TRACKING_PREFIX = CCT_MODULE_PREFIX + "TRACKED_KEYS:" ;
const std::string CCT_MODULE_QUERY_PREFIX = CCT_MODULE_PREFIX + "TRACKED_QUERIES:" ;
const std::string CCT_MODULE_CLIENT_QUERY_PREFIX =  CCT_MODULE_PREFIX + "TRACKED_CLIENT_QUERIES:" ;
const std::string CCT_MODULE_KEY_SEPERATOR  = ":" ;
const std::string CCT_MODULE_KEY_LEVEL = "." ;
const std::string CCT_MODULE_KEY_LEVEL_WITH_ESCAPE = "\\." ;
const int CLIENT_OFFLINE = 0 ;

const std::unordered_map<std::string, std::string> CCT_KEY_EVENTS = 
                                                                    {
                                                                        {"json.set", "UPDATE"},
                                                                        {"del", "DELETE"},
                                                                        {"expire", "EXPIRE"}
                                                                    };

const int MS_MULT = 1000 ; 
const int CCT_TTL = 10 * MS_MULT ; // Seconds

#endif /* CONSTANTS_H */