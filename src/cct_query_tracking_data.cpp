#include <errno.h>
#include <string.h>
#include <vector>

#include "logger.h"
#include "constants.h"
#include "query_parser.h"
#include "cct_client_tracker.h"
#include "json_handler.h"
#include "cct_query_tracking_data.h"
#include "cct_index_tracker.h"
#include "cct_command_subscribe_index.h"
#include "cct_command_register.h"
void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_tracking_group, const std::vector<std::string> &key_ids, const std::string index) {
    RedisModule_AutoMemory(ctx);
    std::string query_term_attribute_normalized = Get_Query_Normalized(query);
    std::string index_and_query = index + CCT_MODULE_KEY_SEPERATOR + query_term_attribute_normalized;

    // Save the Query:{Clients}
    std::string query_tracking_key_str = CCT_MODULE_QUERY_2_CLIENT + index_and_query;
    RedisModuleCallReply *sadd_reply_client_query_to_client = RedisModule_Call(ctx, "SADD", "cc", query_tracking_key_str.c_str()  , client_tracking_group.c_str());
    if (RedisModule_CallReplyType(sadd_reply_client_query_to_client) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Query:{Clients} " +  query_tracking_key_str);
    }

    // Save the Client:{Queries}
    std::string client_to_query_key_str = CCT_MODULE_CLIENT_2_QUERY + client_tracking_group;
    RedisModuleCallReply *sadd_reply_client_to_query = RedisModule_Call(ctx, "SADD", "cc", client_to_query_key_str.c_str()  , index_and_query.c_str());
    if (RedisModule_CallReplyType(sadd_reply_client_to_query) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Client:{Queries} " +  client_to_query_key_str);
    }

    // Save the Query:{Keys}
    std::string query_client_key_name_str = CCT_MODULE_QUERY_2_KEY + index_and_query ;
    for (const auto& it : key_ids) {      
        RedisModuleCallReply *sadd_reply_key = RedisModule_Call(ctx, "SADD", "cc", query_client_key_name_str.c_str()  , it.c_str());
        if (RedisModule_CallReplyType(sadd_reply_key) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering query Query:{Keys} " +  query_client_key_name_str);
        }        
    }

    // Save the Key:{Queries}
    for (const auto& it : key_ids) {
        std::string key_key_name_str = CCT_MODULE_KEY_2_QUERY + it;  
        RedisModuleCallReply *sadd_reply_key = RedisModule_Call(ctx, "SADD", "cc", key_key_name_str.c_str()  , index_and_query.c_str());
        if (RedisModule_CallReplyType(sadd_reply_key) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Key:{Queries} " +  index_and_query);
        }
    }

    // Save the "Query:Client":1 Expire
    std::string query_client_expire_key_name_str = CCT_MODULE_QUERY_CLIENT + index_and_query + CCT_MODULE_KEY_SEPERATOR + client_tracking_group;
    RedisModuleString *query_client_expire_key_name = RedisModule_CreateString(ctx, query_client_expire_key_name_str.c_str() , query_client_expire_key_name_str.length());
    RedisModuleString *empty = RedisModule_CreateString(ctx, "1" , 1);
    RedisModuleKey *query_client_key = RedisModule_OpenKey(ctx, query_client_expire_key_name, REDISMODULE_WRITE);
    if(RedisModule_StringSet(query_client_key, empty) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Query:Client:1 " +  query_client_expire_key_name_str);
    }

    ClientTracker& client_tracker = ClientTracker::getInstance();
    if(RedisModule_SetExpire(query_client_key, client_tracker.getClientQueryTTL(client_tracking_group)) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed set expire for Query:Client:1  " +  query_client_expire_key_name_str);
    }

}

void Update_Tracking_Query(RedisModuleCtx *ctx, const std::string query_str, const std::string new_key) {
    RedisModule_AutoMemory(ctx);

    // Update Query:{Keys}
    std::string query_key_key_name_str = CCT_MODULE_QUERY_2_KEY + query_str;
    RedisModuleCallReply *sadd_reply_qk = RedisModule_Call(ctx, "SADD", "cc", query_key_key_name_str.c_str(), new_key.c_str());
    if (RedisModule_CallReplyType(sadd_reply_qk) != REDISMODULE_REPLY_INTEGER ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Update_Tracking_Query failed while registering query Query:{Keys} " +  query_key_key_name_str);
    }

    // Update the Key:{Queries}
    std::string key_key_name_str = CCT_MODULE_KEY_2_QUERY + new_key;
    RedisModuleCallReply *sadd_reply_kq = RedisModule_Call(ctx, "SADD", "cc", key_key_name_str.c_str(), query_str.c_str());
    if (RedisModule_CallReplyType(sadd_reply_kq) != REDISMODULE_REPLY_INTEGER ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Key:{Queries} " +  query_str);
    }
}

void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client_tracking_group, bool renew) {
    RedisModule_AutoMemory(ctx);

    std::string key_with_prefix = CCT_MODULE_KEY_2_CLIENT + key;
    if(!renew) {
        // Save the Key:{Clients} Expire
        RedisModuleCallReply *sadd_key_reply = RedisModule_Call(ctx, "SADD", "cc", key_with_prefix.c_str()  , client_tracking_group.c_str());
        if (RedisModule_CallReplyType(sadd_key_reply) != REDISMODULE_REPLY_INTEGER ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Key failed while registering tracking key: " +  key_with_prefix);
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Tracking_Key Client : " + client_tracking_group  + " is added to set: " + key_with_prefix);
        }
    }
    
    RedisModuleString *key_tracking_set_key_str = RedisModule_CreateString(ctx, key_with_prefix.c_str() , key_with_prefix.length());
    RedisModuleKey *key_tracking_set_key = RedisModule_OpenKey(ctx, key_tracking_set_key_str, REDISMODULE_WRITE);
    if(RedisModule_KeyType(key_tracking_set_key) == REDISMODULE_KEYTYPE_EMPTY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Key failed set expire for client tracking set (key null): " +  key_with_prefix);
    } 

    // Set new TTL
    ClientTracker& client_tracker = ClientTracker::getInstance();
    long long new_ttl = client_tracker.getClientQueryTTL(client_tracking_group);
    // Get previous TTL 
    long long current_ttl = RedisModule_GetExpire(key_tracking_set_key);
    if( new_ttl < current_ttl) {
        new_ttl = current_ttl;
    }
    // This is best effort cleaning
    if(RedisModule_SetExpire(key_tracking_set_key, new_ttl) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Key failed set expire for client tracking set: " +  key_with_prefix + " with ttl value : " + std::to_string(new_ttl));
    }
}

void Add_Tracking_Key_Old_Value(RedisModuleCtx *ctx, std::string key, std::string value, bool delete_old) {
    RedisModule_AutoMemory(ctx);
    std::string old_key_with_prefix = CCT_MODULE_KEY_OLD_VALUE + key;
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Tracking_Key_Old_Value has called with key : " +  old_key_with_prefix);

    RedisModuleString *old_key_str = RedisModule_CreateString(ctx, old_key_with_prefix.c_str() , old_key_with_prefix.length());
    RedisModuleKey *old_key = RedisModule_OpenKey(ctx, old_key_str, REDISMODULE_WRITE);

    if(delete_old) { // Key is delete we don't need the old value anymore
        if( RedisModule_DeleteKey(old_key) == REDISMODULE_ERR ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Deleting old valu key failed : " +  old_key_with_prefix);
        }
        return;
    }

    RedisModuleString *value_str = RedisModule_CreateString(ctx, value.c_str() , value.length());

    if(RedisModule_StringSet(old_key, value_str) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Key_Old_Value failed while writing to key : " +  old_key_with_prefix);
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Tracking_Key_Old_Value has written key : " +  old_key_with_prefix);

    }
}

int Add_Event_To_Stream(RedisModuleCtx *ctx, const std::string stream_name, const std::string event, const std::string key, const std::string value, const std::string queries, bool send_old_value, bool index_subscription, bool snapshot ) { 
    RedisModule_AutoMemory(ctx);
    ClientTracker& client_tracker = ClientTracker::getInstance();
    if (!index_subscription) {
        if( client_tracker.isClientConnected(stream_name) == false) {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Event_To_Stream skipping offline client : " + stream_name);
            return REDISMODULE_OK;
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Event_To_Stream adding for client:  " + stream_name);
        }
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Event_To_Stream adding for index subscription stream:  " + stream_name);
    }
    RedisModuleString *client_name = RedisModule_CreateString(ctx, stream_name.c_str(), stream_name.length());
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);

    
    if(!snapshot && Is_Snapshot_InProgress(stream_name)) {
        Add_Snapshot_Event(stream_name, event, key, value, queries, send_old_value);
        return REDISMODULE_OK;
    }

    int alloc_count = 8;
    if (send_old_value) {
        alloc_count = 10;
    }
    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * alloc_count);
    xadd_params[0] = RedisModule_CreateString(ctx, CCT_OPERATION.c_str(), strlen(CCT_OPERATION.c_str()));
    std::string internal_event = CCT_KEY_EVENTS.at(event);
    xadd_params[1] = RedisModule_CreateString(ctx, internal_event.c_str(), internal_event.length());
    xadd_params[2] = RedisModule_CreateString(ctx, CCT_KEY.c_str(), strlen(CCT_KEY.c_str()));
    if(!key.empty()) {
        xadd_params[3] = RedisModule_CreateString(ctx, key.c_str(), key.length());
    } else {
        xadd_params[3] = RedisModule_CreateString(ctx, "", strlen(""));
    }
    xadd_params[4] = RedisModule_CreateString(ctx, CCT_VALUE.c_str(), strlen(CCT_VALUE.c_str()));
    xadd_params[5] = RedisModule_CreateString(ctx, value.c_str(), value.length());
    xadd_params[6] = RedisModule_CreateString(ctx, CCT_QUERIES.c_str(), strlen(CCT_QUERIES.c_str()));
    xadd_params[7] = RedisModule_CreateString(ctx, queries.c_str(), queries.length());
    if (send_old_value) {
        std::string old_value_key = CCT_MODULE_KEY_OLD_VALUE + key;
        RedisModuleCallReply *get_reply = RedisModule_Call(ctx,"GET","c", old_value_key.c_str());
        std::string old_value = "";
        if (RedisModule_CallReplyType(get_reply) == REDISMODULE_REPLY_ERROR){
            old_value = "";
        } else if ( RedisModule_CallReplyType(get_reply) == REDISMODULE_REPLY_NULL ) {
            old_value = "";
        } else {
            size_t len;
            old_value = RedisModule_CallReplyStringPtr(get_reply, &len);
        }
        xadd_params[8] = RedisModule_CreateString(ctx, CCT_OLD_VALUE.c_str(), strlen(CCT_OLD_VALUE.c_str()));
        xadd_params[9] = RedisModule_CreateString(ctx, old_value.c_str(), strlen(old_value.c_str()));  
    }
    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, (alloc_count/2));
    RedisModule_Free(xadd_params);
    if (stream_add_resp != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Event_To_Stream failed to add the stream.");
        return REDISMODULE_ERR;
    }
    // Now write new value to backup
    if (send_old_value) {
        if(queries.empty()) {
            Add_Tracking_Key_Old_Value(ctx, key, value, true);
        } else {
            Add_Tracking_Key_Old_Value(ctx, key, value, false);
        } 
    }
    return REDISMODULE_OK;
}


int Trim_Stream_By_ID(RedisModuleCtx *ctx, RedisModuleString *last_read_id, std::string client_name) {
    RedisModule_AutoMemory(ctx);
    RedisModuleStreamID minid;
    if (RedisModule_StringToStreamID(last_read_id, &minid) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Trim_Stream_By_ID:  Provided Stream ID is not valid.");
        return REDISMODULE_ERR;
    }

    minid.seq = minid.seq + 1; // We get last read so delete the last read too 

    RedisModuleString *client_name_r = RedisModule_CreateString(ctx, client_name.c_str(), client_name.length());
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name_r, REDISMODULE_WRITE);

    long long trim_resp = RedisModule_StreamTrimByID(stream_key, 0, &minid);
    if (trim_resp < 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Trim_Stream_By_ID:  Trim with given min id failed : " + std::to_string(minid.seq) + " for client : " + client_name + " with response : " + std::to_string(trim_resp));
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

void Handle_Deleted_Key(RedisModuleCtx *ctx, const std::string deleted_key) {
	    
    // First get the queries matching the deleted key
    std::set<std::string> tracked_queries_for_key;
    std::string k2q = CCT_MODULE_KEY_2_QUERY + deleted_key;
    RedisModuleCallReply *k2q_smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", k2q.c_str());
    const size_t reply_length = RedisModule_CallReplyLength(k2q_smembers_reply);
    for (size_t i = 0; i < reply_length; i++) {
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(k2q_smembers_reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING) {
            RedisModuleString *query_name = RedisModule_CreateStringFromCallReply(key_reply);
            const char *query_name_str = RedisModule_StringPtrLen(query_name, NULL);
            tracked_queries_for_key.insert(std::string(query_name_str));
        }
    }

    // Now delete the Key:Queries 
    RedisModuleString *k2q_key_str = RedisModule_CreateString(ctx, k2q.c_str() , k2q.length());
    RedisModuleKey *k2q_key = RedisModule_OpenKey(ctx, k2q_key_str, REDISMODULE_WRITE);
    if( RedisModule_DeleteKey(k2q_key) == REDISMODULE_ERR ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Deleted_Key deleting key:queries failed : " +  k2q);
    }

    for (auto &query_to_delete : tracked_queries_for_key) {
        std::string q2k_key = CCT_MODULE_QUERY_2_KEY + query_to_delete;
        RedisModuleCallReply *q2k_srem_key_reply = RedisModule_Call(ctx, "SREM", "cc", q2k_key.c_str(), deleted_key.c_str());
        if (RedisModule_CallReplyType(q2k_srem_key_reply) != REDISMODULE_REPLY_INTEGER) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Deleted_Key (Query:{Keys}) failed while deleting key: " +  deleted_key);
            continue; ;
        } else if ( RedisModule_CallReplyInteger(q2k_srem_key_reply) == 0 ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handle_Deleted_Key (Query:{Keys}) failed while deleting key (non existing key): " +  deleted_key);
            continue ;
        }
    }

}


void Renew_Queries(RedisModuleCtx *ctx, std::vector<std::string> queries, const std::string client_tracking_group, unsigned long long client_ttl) {
    RedisModule_AutoMemory(ctx);
    
    for (const auto& query : queries) {
        // Renew the Query:Client:1
        std::string query_client_expire_key_name_str = CCT_MODULE_QUERY_CLIENT + query + CCT_MODULE_KEY_SEPERATOR + client_tracking_group;
        RedisModuleString *query_client_expire_key_name = RedisModule_CreateString(ctx, query_client_expire_key_name_str.c_str() , query_client_expire_key_name_str.length());
        if (RedisModule_KeyExists(ctx, query_client_expire_key_name)){
            RedisModuleKey *query_client_key = RedisModule_OpenKey(ctx, query_client_expire_key_name, REDISMODULE_WRITE);
            if(RedisModule_SetExpire(query_client_key, client_ttl) != REDISMODULE_OK){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Renew_Queries failed set expire for Query:Client:1  " +  query_client_expire_key_name_str);
            }
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Renew_Queries set expire for Query:Client:1  " +  query_client_expire_key_name_str);
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Renew_Queries failed to find Query:Client:1  " +  query_client_expire_key_name_str);
        }

        // Get the Query:{Keys}
        std::vector<std::string> keys_matching_query;
        std::string q2k = CCT_MODULE_QUERY_2_KEY + query;
        RedisModuleCallReply *q2k_smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", q2k.c_str());
        const size_t reply_length = RedisModule_CallReplyLength(q2k_smembers_reply);
        for (size_t i = 0; i < reply_length; i++) {
            RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(q2k_smembers_reply, i);
            if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
                RedisModuleString *key_name = RedisModule_CreateStringFromCallReply(key_reply);
                const char *key_name_str = RedisModule_StringPtrLen(key_name, NULL);
                keys_matching_query.push_back(std::string(key_name_str));
            }
        }

        // Renew the Key:{Clients}
        for (auto &key_name : keys_matching_query) {
            Add_Tracking_Key(ctx, key_name, client_tracking_group, true);
        }
    }

}

std::string Get_Key_Queries(RedisModuleCtx *ctx, const std::string key) {
    std::set<std::string> tracked_queries_for_key;
    std::string k2q_key = CCT_MODULE_KEY_2_QUERY + key;
    RedisModuleCallReply *k2q_smembers_reply = RedisModule_Call(ctx, "SMEMBERS", "c", k2q_key.c_str());
    const size_t reply_length = RedisModule_CallReplyLength(k2q_smembers_reply);
    for (size_t i = 0; i < reply_length; i++) {
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(k2q_smembers_reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING) {
            RedisModuleString *query_name = RedisModule_CreateStringFromCallReply(key_reply);
            const char *query_name_str = RedisModule_StringPtrLen(query_name, NULL);
            std::string query_str = Normalized_to_Original_With_Index(query_name_str);
            tracked_queries_for_key.insert(query_str);
        }
    }
    return Concate_Queries(tracked_queries_for_key);
}

void Add_Subscribed_Index(RedisModuleCtx *ctx, const std::string index_name) {
    RedisModule_AutoMemory(ctx);
    RedisModuleCallReply *sadd_reply = RedisModule_Call(ctx, "SADD", "cc", CCT_MODULE_SUBSCRIBED_INDEX.c_str(), index_name.c_str());
    if (RedisModule_CallReplyType(sadd_reply) != REDISMODULE_REPLY_INTEGER) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Subscribed_Index failed while adding index: " +  index_name);
    }
}

void Remove_Subscribed_Index(RedisModuleCtx *ctx, const std::string index_name) {
    RedisModule_AutoMemory(ctx);
    RedisModuleCallReply *srem_reply = RedisModule_Call(ctx, "SREM", "cc", CCT_MODULE_SUBSCRIBED_INDEX.c_str(), index_name.c_str());
    if (RedisModule_CallReplyType(srem_reply) != REDISMODULE_REPLY_INTEGER) {   
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Remove_Subscribed_Index failed while removing index: " +  index_name);
    }
}

void Add_Tracked_Index_Event_To_Stream(RedisModuleCtx *ctx, const std::string index_name, const std::string event, const std::string key){
    RedisModule_AutoMemory(ctx);

    std::string index_subcription_query = index_name + ":*";

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Tracked_Index_Event_To_Stream added event to stream: " +  index_name + " for event: " + event + " and key: " + key);

    if(strcasecmp(event.c_str(), "expired") == 0 || strcasecmp(event.c_str(), "del") == 0) {
        if ( REDISMODULE_OK != Add_Event_To_Stream(ctx, index_name, event, key, "", index_subcription_query, false, true) ) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracked_Index_Event_To_Stream failed while adding event to stream: " +  index_name + " for event: " + event + " and key: " + key);
        }
        return;
    }

    RedisModuleCallReply *get_reply = RedisModule_Call(ctx, "JSON.GET", "c", key.c_str());
    if (RedisModule_CallReplyType(get_reply) != REDISMODULE_REPLY_STRING) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracked_Index_Event_To_Stream failed while getting value for key: " +  key);
        return;
    }
    std::string value = RedisModule_CallReplyStringPtr(get_reply, NULL);

    if ( REDISMODULE_OK != Add_Event_To_Stream(ctx, index_name, "json.set", key, value, index_subcription_query, false, true) ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracked_Index_Event_To_Stream failed while adding event to stream: " +  index_name + " for event: " + event + " and key: " + key);
        return;
    }

}