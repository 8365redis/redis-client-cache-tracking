#include <errno.h>
#include <string.h>
#include <vector>
#include <string>
#include <set>
#include <iostream>

#include "cct_query_tracking_logic.h"
#include "query_parser.h"
#include "client_tracker.h"
#include "cct_index_tracker.h"
#include "logger.h"
#include "constants.h"
#include "json_handler.h"
#include "cct_query_tracking_data.h"
#include "cct_query_expiry_logic.h"

int Get_Tracking_Clients_From_Changed_JSON(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key,
                                             std::vector<std::string> &clients_to_update, std::string &json_str, 
                                             std::unordered_map<std::string, std::vector<std::string>> &client_to_queries_map,
                                             std::set<std::string> &current_queries) {
    RedisModule_AutoMemory(ctx);

    std::vector<std::string> queries;
    std::string key_str = RedisModule_StringPtrLen(r_key, NULL);

    // Add the wildcard query for the index if there is any
    std::set<std::string> tracked_indexes = Get_Tracked_Indexes_From_Key(key_str);
    if( !tracked_indexes.empty() ) {
        queries.push_back(WILDCARD_SEARCH);
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON add the wildcard query for key: " + key_str );
    }
    
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON :  key " + key_str + " for event: " + event);

    if(event == REDIS_DEL_EVENT) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON :  Delete event for key : " + key_str + " return empty");
    } else {
        RedisModuleString *value = Get_JSON_Value(ctx, event , r_key);
        if (value == NULL){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Tracking_Clients_From_Changed_JSON failed while getting JSON value for key: " +  key_str);
            return REDISMODULE_ERR;
        }

        json_str = RedisModule_StringPtrLen(value, NULL);
        if ( json_str.empty() ){ 
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON :  JSON value is empty for key: " + key_str);
            return REDISMODULE_OK;
        }

        nlohmann::json json_object = Get_JSON_Object(ctx, json_str);
        if(json_object == NULL) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Tracking_Clients_From_Changed_JSON failed to parse JSON for key: " +  key_str);
            return REDISMODULE_ERR;
        }
        Recursive_JSON_Iterate(json_object , "", queries);
    }

    std::set<std::string> indexes = Get_Indexes_From_Key(key_str);
    for (const auto &index : indexes) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON checking for index: " + index);
        for (auto & q : queries) {
            std::string escaped_query = Escape_FtQuery(q);
            std::string index_and_query = index + CCT_MODULE_KEY_SEPERATOR + escaped_query;
            current_queries.insert(index_and_query);
            std::string query_with_prefix = CCT_MODULE_QUERY_2_CLIENT + index_and_query;
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON check this query for tracking: " + query_with_prefix);
            RedisModuleCallReply *smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", query_with_prefix.c_str());

            if (RedisModule_CallReplyType(smembers_reply) != REDISMODULE_REPLY_ARRAY ) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Get_Tracking_Clients_From_Changed_JSON failed while getting client names for query: " +  query_with_prefix);
                return REDISMODULE_ERR;
            } else {
                const size_t reply_length = RedisModule_CallReplyLength(smembers_reply);
                for (size_t i = 0; i < reply_length; i++) {
                    RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(smembers_reply, i);
                    if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING) {
                        RedisModuleString *client = RedisModule_CreateStringFromCallReply(key_reply);
                        const char *client_str = RedisModule_StringPtrLen(client, NULL);
                        clients_to_update.push_back(std::string(client_str));
                        client_to_queries_map[std::string(client_str)].push_back(index_and_query);
                        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Tracking_Clients_From_Changed_JSON query matched to this client: " + (std::string)client_str);
                        Add_Tracking_Key(ctx, key_str, client_str);
                        Update_Tracking_Query(ctx, index_and_query, key_str);
                    }
                }
            }
        }
    }
    return REDISMODULE_OK;
}

// In this context client = client_tracking_group
int Query_Track_Check(RedisModuleCtx *ctx, std::string event, RedisModuleString* r_key, std::vector<std::string> already_tracking_clients) {
    RedisModule_AutoMemory(ctx);

    std::string key_str = RedisModule_StringPtrLen(r_key, NULL);
    std::set<std::string> indexes = Get_Indexes_From_Key(key_str);

    std::vector<std::string> clients_to_update;
    std::string json_str;
    std::unordered_map<std::string, std::vector<std::string>> client_to_queries_map;
    std::set<std::string> current_queries_for_key;
    Get_Tracking_Clients_From_Changed_JSON(ctx , event,  r_key , clients_to_update , json_str, client_to_queries_map, current_queries_for_key);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check event : " +  event + " , key  : " + key_str + " , indexes : " + Set_To_String(indexes));
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check already_tracking_clients : " +  Vector_To_String(already_tracking_clients));
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check clients_to_update : " +  Vector_To_String(clients_to_update));
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check current_queries_for_key : " +  Set_To_String(current_queries_for_key));

    std::set<std::string> already_tracking_clients_set(already_tracking_clients.begin(), already_tracking_clients.end());
    std::set<std::string> clients_to_update_set(clients_to_update.begin(), clients_to_update.end());

    std::set<std::string> total_clients;
    std::set_union(std::begin(already_tracking_clients_set), std::end(already_tracking_clients_set), std::begin(clients_to_update_set), std::end(clients_to_update_set), std::inserter(total_clients, std::begin(total_clients)));    
    // Write to stream
    for (auto & client_name : total_clients) {
        auto client_queries = client_to_queries_map[client_name];
        std::string client_queries_str;
        std::vector<std::string> client_queries_original;
        for (auto q : client_queries) {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check Client : " + client_name + " , and query is : " +  q);
            std::string index_and_query = Normalized_to_Original_With_Index(q);
            client_queries_original.push_back(index_and_query);
        }
        for(auto const& e : client_queries_original) client_queries_str += (e + CCT_MODULE_QUERY_DELIMETER);
        if(client_queries_str.length() > CCT_MODULE_QUERY_DELIMETER.length()){
            client_queries_str.erase(client_queries_str.length() - CCT_MODULE_QUERY_DELIMETER.length());
        }

        ClientTracker& client_tracker = ClientTracker::getInstance();
        std::set<std::string> c_s = client_tracker.getClientTrackingGroupClients(client_name);
        for (auto &c : c_s) {
            if (Add_Event_To_Stream(ctx, c, event, key_str, json_str, client_queries_str, cct_config.CCT_SEND_OLD_VALUE_CFG) != REDISMODULE_OK) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed to adding to the stream." );
                return RedisModule_ReplyWithError(ctx, strerror(errno));
            }
        }
    }
    // Now delete the tracked keys which are not matching to our queries anymore
    std::set<std::string> diff_clients;
    // already_tracking_clients_set - clients_to_update_set
    std::set_difference (already_tracking_clients_set.begin(), already_tracking_clients_set.end(), clients_to_update_set.begin(), clients_to_update_set.end(), inserter(diff_clients, diff_clients.begin()));
    // Delete no more tracked keys
    for (const auto& it : diff_clients) {
        std::string key_with_prefix = CCT_MODULE_KEY_2_CLIENT + key_str;
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check will delete no more interested client: " + it + " from tracked key : " +  key_with_prefix);
        RedisModuleCallReply *srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", key_with_prefix.c_str()  , it.c_str());
        if (RedisModule_CallReplyType(srem_key_reply) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check failed while deleting tracking key: " +  key_with_prefix);
            return REDISMODULE_ERR;
        }
    }

    //For updates, compute and delete CCT metadata for queries that no longer match our new JSON value
	if (CCT_KEY_EVENTS.at(event) == CCT_UPDATE_EVENT) {
	    // Get all the queries that we track for the key
	    std::set<std::string> tracked_queries_for_key;
	    std::string k2q_key = CCT_MODULE_KEY_2_QUERY + key_str;
	    RedisModuleCallReply *k2q_smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", k2q_key.c_str());
	    const size_t reply_length = RedisModule_CallReplyLength(k2q_smembers_reply);
	    for (size_t i = 0; i < reply_length; i++) {
	        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(k2q_smembers_reply, i);
	        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING) {
	            RedisModuleString *query_name = RedisModule_CreateStringFromCallReply(key_reply);
	            const char *query_name_str = RedisModule_StringPtrLen(query_name, NULL);
	            tracked_queries_for_key.insert(std::string(query_name_str));
	        }
	    }
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check tracked_queries_for_key : " +  Set_To_String(tracked_queries_for_key));
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check current_queries_for_key : " +  Set_To_String(current_queries_for_key));
		// Compute all tracked queries that no longer match the new JSON value (tracked_queries_for_key - current_queries_for_key)
    	std::set<std::string> queries_no_longer_matching_value;
    	std::set_difference(tracked_queries_for_key.begin(), tracked_queries_for_key.end(),
                        	current_queries_for_key.begin(), current_queries_for_key.end(),
                        	std::inserter(queries_no_longer_matching_value, queries_no_longer_matching_value.begin()));

        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Query_Track_Check queries_no_longer_matching_value : " +  Set_To_String(queries_no_longer_matching_value));

    	// Delete all the tracked queries that no longer match the JSON value
    	for (auto &query_to_delete : queries_no_longer_matching_value) {
            std::string q2k_key = CCT_MODULE_QUERY_2_KEY + query_to_delete;
            RedisModuleCallReply *q2k_srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", q2k_key.c_str(), key_str.c_str());
            if (RedisModule_CallReplyType(q2k_srem_key_reply) != REDISMODULE_REPLY_INTEGER) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check (Query:{Keys}) failed while deleting key: " +  key_str);
                return REDISMODULE_ERR;
            } else if ( RedisModule_CallReplyInteger(q2k_srem_key_reply) == 0 ) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check (Query:{Keys}) failed while deleting key (non existing key): " +  key_str);
                return REDISMODULE_ERR;
            }
            std::string k2q_key = CCT_MODULE_KEY_2_QUERY + key_str;
            RedisModuleCallReply *k2q_srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", k2q_key.c_str(), query_to_delete.c_str());
            if (RedisModule_CallReplyType(k2q_srem_key_reply) != REDISMODULE_REPLY_INTEGER){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check (Key:{Queries}) failed while deleting query: " +  query_to_delete);
                return REDISMODULE_ERR;
            } else if ( RedisModule_CallReplyInteger(k2q_srem_key_reply) == 0 ) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Query_Track_Check (Key:{Queries}) failed while deleting query (non existing key): " +  query_to_delete);
                return REDISMODULE_ERR;
            }
    	}
	} else if (CCT_KEY_EVENTS.at(event) == CCT_DELETE_EVENT){
        Handle_Deleted_Key(ctx, key_str);
    }

    return REDISMODULE_OK;
}

int Notify_Callback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    RedisModule_AutoMemory(ctx);

    std::string event_str = event;
    std::string key_str = RedisModule_StringPtrLen(key, NULL);

    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str);

    if( CCT_KEY_EVENTS.count(event) == 0 )
    {
        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " not interested event." );
        return REDISMODULE_OK;        
    }

    // Ignore our self events
    if (key_str.find(CCT_MODULE_PREFIX, 0) == 0) {
        if(strcasecmp(event, "expired") != 0) {
            //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
            return REDISMODULE_OK;
        } else if(key_str.find(CCT_MODULE_QUERY_CLIENT, 0) == 0) {
            return Handle_Query_Expire(ctx, key_str);           
        } else {
            //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Notify_Callback event : " + event_str  + " , key " + key_str + " ignore our own events to prevent loops." );
            return REDISMODULE_OK;
        }
    }

    // Add prefix
    std::string key_with_prefix = CCT_MODULE_KEY_2_CLIENT + key_str;

    // First check which clients are tracking updated key
    std::vector<std::string> already_tracking_clients;
    RedisModuleCallReply *smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", key_with_prefix.c_str());
    const size_t reply_length = RedisModule_CallReplyLength(smembers_reply);
    for (size_t i = 0; i < reply_length; i++) {
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(smembers_reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *stream_name = RedisModule_CreateStringFromCallReply(key_reply);
            const char *stream_name_str = RedisModule_StringPtrLen(stream_name, NULL);
            already_tracking_clients.push_back(std::string(stream_name_str));
        }
    }

    Query_Track_Check(ctx, event_str, key, already_tracking_clients);
    
    return REDISMODULE_OK;
}