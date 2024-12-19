#pragma once

#include <set>
#include <string>
#include "redismodule.h"

class Redis_Index_Manager {
public:
    // Singleton accessor
    static Redis_Index_Manager& Instance() {
        static Redis_Index_Manager instance;
        return instance;
    }

    // Deleted copy constructor and assignment operator
    Redis_Index_Manager(const Redis_Index_Manager&) = delete;
    Redis_Index_Manager& operator=(const Redis_Index_Manager&) = delete;

    std::set<std::string> Get_All_Indexes(RedisModuleCtx *ctx);
    void Get_Index_Prefixes(RedisModuleCtx *ctx, std::set<std::string> indexes);
    void Set_Index_Change(const bool change);
    bool Get_Index_Change() const;

private:
    // Private constructor for singleton pattern
    Redis_Index_Manager() = default;
    bool active_index_change = false;
};

void OnRedisReady(RedisModuleCtx *ctx, RedisModuleEvent event, uint64_t subevent, void *data);
std::set<std::string> Get_Tracked_Indexes_From_Key(std::string key);
void Track_Index(RedisModuleCtx *ctx, std::string index);
void UnTrack_Index(RedisModuleCtx *ctx, std::string index);
bool Is_Index_Tracked(std::string index);
std::set<std::string> Get_Indexes_From_Key(std::string key);

void Start_Index_Change_Handler(RedisModuleCtx *ctx);
void Index_Change_Handler(RedisModuleCtx *ctx);

void Load_Subscribed_Indexes(RedisModuleCtx *ctx);
