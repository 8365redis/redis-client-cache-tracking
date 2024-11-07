#pragma once

#include <string>
#include "redismodule.h"
#include "constants.h"

const std::string DEFAULT_CONFIG_FILE_NAME  = "cct2-config.ini";
const std::string DEFAULT_CONFIG_SECTION  = "cct";

const std::string CCT_QUERY_TTL_SECOND_CONFIG  = "CCT_QUERY_TTL_SECOND";
const std::string CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CONFIG = "CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND";
const std::string CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CONFIG = "CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT";
const std::string CCT_SEND_OLD_VALUE_CONFIG = "CCT_SEND_OLD_VALUE";
const std::string CCT_AGGREGATE_HANDLER_CONFIG = "CCT_AGGREGATE_HANDLER";
const std::string CCT_AGGREGATE_HANDLER_INTERVAL_SECOND_CONFIG = "CCT_AGGREGATE_HANDLER_INTERVAL_SECOND";

CCT_Config Read_CCT_Config(RedisModuleCtx *ctx, std::string config_file_path_str);
