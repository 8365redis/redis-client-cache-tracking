#ifndef CONFIG_HANDLER_H
#define CONFIG_HANDLER_H

#include <string>
#include "redismodule.h"
#include "constants.h"

const std::string DEFAULT_CONFIG_FILE_NAME  = "cct2-config.ini";
const std::string DEFAULT_CONFIG_SECTION  = "cct";

const std::string CCT_QUERY_TTL_SECOND_CONFIG  = "CCT_QUERY_TTL_SECOND";
const std::string CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CONFIG = "CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND";
const std::string CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CONFIG = "CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT";
const std::string CCT_SEND_OLD_VALUE_CONFIG = "CCT_SEND_OLD_VALUE";

CCT_Config Read_CCT_Config(RedisModuleCtx *ctx, std::string config_file_path_str);


#endif /* CONFIG_HANDLER_H */