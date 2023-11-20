#include <dlfcn.h>
#include "config_handler.h"
#include "ini.h"
#include "logger.h"

CCT_Config Read_CCT_Config(RedisModuleCtx *ctx, std::string config_file_path_str) {
    CCT_Config cct_config;
    std::string config_file_full_path;
    if(config_file_path_str.empty()) {
        Dl_info dl_info;
        dladdr((void*)Read_CCT_Config, &dl_info);
        std::string  module_lib_path = dl_info.dli_fname;
        size_t found = module_lib_path.find_last_of("/");
        std::string directory_name = module_lib_path.substr(0,found+1);
        config_file_full_path =  directory_name + DEFAULT_CONFIG_FILE_NAME;
    }else {
        config_file_full_path = config_file_path_str;
    }
    mINI::INIFile file(config_file_full_path);
    mINI::INIStructure config;
    bool config_read = file.read(config);

    if(config_read == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config file is not loaded (using default values) : " + config_file_full_path);
    } else {
        if(config_file_path_str.empty()) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config file is read from default config file." );
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config file provided as argument." );
        }
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config values read from file: " + config_file_full_path);

        if(config[DEFAULT_CONFIG_SECTION].size() == 0) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config section " + DEFAULT_CONFIG_SECTION + " is missing. Using default values.");
            return cct_config;
        }

        if( !config[DEFAULT_CONFIG_SECTION][CCT_QUERY_TTL_SECOND_CONFIG].empty() ) {
            try{
                cct_config.CCT_QUERY_TTL_SECOND_CFG = std::stoi(config[DEFAULT_CONFIG_SECTION][CCT_QUERY_TTL_SECOND_CONFIG]);
            } catch (std::invalid_argument const& ex) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value " + CCT_QUERY_TTL_SECOND_CONFIG  + " has failed to read value.");
            }
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [CCT_QUERY_TTL_SECOND_CFG] : " + std::to_string(cct_config.CCT_QUERY_TTL_SECOND_CFG));
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [CCT_QUERY_TTL_SECOND_CFG] is not found in config file using default.");
        }

        if( !config[DEFAULT_CONFIG_SECTION][CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CONFIG].empty() ) {
            try{
                cct_config.CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CFG = std::stoi(config[DEFAULT_CONFIG_SECTION][CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CONFIG]);
            } catch (std::invalid_argument const& ex) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value " + CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CONFIG  + " has failed to read value.");
            }
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CFG] : " + std::to_string(cct_config.CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CFG));
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND_CFG] is not found in config file using default.");
        }


        if( !config[DEFAULT_CONFIG_SECTION][CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CONFIG].empty() ) {
            try{
                cct_config.CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CFG = std::stoi(config[DEFAULT_CONFIG_SECTION][CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CONFIG]);
            } catch (std::invalid_argument const& ex) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value " + CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CONFIG  + " has failed to read value.");
            }
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CFG] : " + std::to_string(cct_config.CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CFG));
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [CCT_CLIENT_TTL_HEARTBEAT_MISS_COUNT_CFG] is not found in config file using default.");
        }
    }
    return cct_config;

}
