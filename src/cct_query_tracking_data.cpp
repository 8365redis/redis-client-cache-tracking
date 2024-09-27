#include "cct_query_tracking_data.h"

void Add_Tracking_Wildcard_Query(RedisModuleCtx *ctx, std::string query_str, std::string client_tracking_group) {
    // Save the Query:{Clients}
    std::string query_tracking_key_str = CCT_MODULE_QUERY_2_CLIENT + query_str;
    RedisModuleCallReply *sadd_reply_client_query_to_client = RedisModule_Call(ctx, "SADD", "cc", query_tracking_key_str.c_str()  , client_tracking_group.c_str());
    if (RedisModule_CallReplyType(sadd_reply_client_query_to_client) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Wildcard_Query failed while registering Query:{Clients} " +  query_tracking_key_str);
    }

    // Save the Client:{Queries}
    std::string client_to_query_key_str = CCT_MODULE_CLIENT_2_QUERY + client_tracking_group;
    RedisModuleCallReply *sadd_reply_client_to_query = RedisModule_Call(ctx, "SADD", "cc", client_to_query_key_str.c_str()  , query_str.c_str());
    if (RedisModule_CallReplyType(sadd_reply_client_to_query) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Wildcard_Query failed while registering Client:{Queries} " +  client_to_query_key_str);
    }

    // Save the "Query:Client":1 Expire
    std::string query_client_expire_key_name_str = CCT_MODULE_QUERY_CLIENT + query_str + CCT_MODULE_KEY_SEPERATOR + client_tracking_group;
    RedisModuleString *query_client_expire_key_name = RedisModule_CreateString(ctx, query_client_expire_key_name_str.c_str() , query_client_expire_key_name_str.length());
    RedisModuleString *empty = RedisModule_CreateString(ctx, "1" , 1);
    RedisModuleKey *query_client_key = RedisModule_OpenKey(ctx, query_client_expire_key_name, REDISMODULE_WRITE);
    if(RedisModule_StringSet(query_client_key, empty) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Wildcard_Query failed while registering Query:Client:1 " +  query_client_expire_key_name_str);
    }

    if(RedisModule_SetExpire(query_client_key, Get_Client_Query_TTL(client_tracking_group)) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Wildcard_Query failed set expire for Query:Client:1  " +  query_client_expire_key_name_str);
    }
}


void Add_Tracking_Query(RedisModuleCtx *ctx, RedisModuleString *query, std::string client_tracking_group, const std::vector<std::string> &key_ids) {

   std::string query_term_attribute_normalized = Get_Query_Normalized(query);

    // Save the Query:{Clients}
    std::string query_tracking_key_str = CCT_MODULE_QUERY_2_CLIENT + query_term_attribute_normalized;
    RedisModuleCallReply *sadd_reply_client_query_to_client = RedisModule_Call(ctx, "SADD", "cc", query_tracking_key_str.c_str()  , client_tracking_group.c_str());
    if (RedisModule_CallReplyType(sadd_reply_client_query_to_client) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Query:{Clients} " +  query_tracking_key_str);
    }

    // Save the Client:{Queries}
    std::string client_to_query_key_str = CCT_MODULE_CLIENT_2_QUERY + client_tracking_group;
    RedisModuleCallReply *sadd_reply_client_to_query = RedisModule_Call(ctx, "SADD", "cc", client_to_query_key_str.c_str()  , query_term_attribute_normalized.c_str());
    if (RedisModule_CallReplyType(sadd_reply_client_to_query) != REDISMODULE_REPLY_INTEGER ){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Client:{Queries} " +  client_to_query_key_str);
    }

    // Save the Query:{Keys}
    std::string query_client_key_name_str = CCT_MODULE_QUERY_2_KEY + query_term_attribute_normalized ;
    for (const auto& it : key_ids) {      
        RedisModuleCallReply *sadd_reply_key = RedisModule_Call(ctx, "SADD", "cc", query_client_key_name_str.c_str()  , it.c_str());
        if (RedisModule_CallReplyType(sadd_reply_key) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering query Query:{Keys} " +  query_client_key_name_str);
        }        
    }

    // Save the Key:{Queries}
    for (const auto& it : key_ids) {
        std::string key_key_name_str = CCT_MODULE_KEY_2_QUERY + it;  
        RedisModuleCallReply *sadd_reply_key = RedisModule_Call(ctx, "SADD", "cc", key_key_name_str.c_str()  , query_term_attribute_normalized.c_str());
        if (RedisModule_CallReplyType(sadd_reply_key) != REDISMODULE_REPLY_INTEGER ){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Key:{Queries} " +  query_term_attribute_normalized);
        }
    }

    // Save the "Query:Client":1 Expire
    std::string query_client_expire_key_name_str = CCT_MODULE_QUERY_CLIENT + query_term_attribute_normalized + CCT_MODULE_KEY_SEPERATOR + client_tracking_group;
    RedisModuleString *query_client_expire_key_name = RedisModule_CreateString(ctx, query_client_expire_key_name_str.c_str() , query_client_expire_key_name_str.length());
    RedisModuleString *empty = RedisModule_CreateString(ctx, "1" , 1);
    RedisModuleKey *query_client_key = RedisModule_OpenKey(ctx, query_client_expire_key_name, REDISMODULE_WRITE);
    if(RedisModule_StringSet(query_client_key, empty) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed while registering Query:Client:1 " +  query_client_expire_key_name_str);
    }

    if(RedisModule_SetExpire(query_client_key, Get_Client_Query_TTL(client_tracking_group)) != REDISMODULE_OK){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Query failed set expire for Query:Client:1  " +  query_client_expire_key_name_str);
    }

}

void Update_Tracking_Query(RedisModuleCtx *ctx, const std::string query_str, const std::string new_key) {
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

void Add_Tracking_Key(RedisModuleCtx *ctx, std::string key, std::string client_tracking_group) {
    // Save the Key:{Clients} Expire
    std::string key_with_prefix = CCT_MODULE_KEY_2_CLIENT + key;
    RedisModuleCallReply *sadd_key_reply = RedisModule_Call(ctx, "SADD", "cc", key_with_prefix.c_str()  , client_tracking_group.c_str());
    if (RedisModule_CallReplyType(sadd_key_reply) != REDISMODULE_REPLY_INTEGER ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Key failed while registering tracking key: " +  key_with_prefix);
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Tracking_Key added to stream: " + key_with_prefix);
    }
    
    RedisModuleString *key_tracking_set_key_str = RedisModule_CreateString(ctx, key_with_prefix.c_str() , key_with_prefix.length());
    RedisModuleKey *key_tracking_set_key = RedisModule_OpenKey(ctx, key_tracking_set_key_str, REDISMODULE_WRITE);
    if(RedisModule_KeyType(key_tracking_set_key) == REDISMODULE_KEYTYPE_EMPTY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Tracking_Key failed set expire for client tracking set (key null): " +  key_with_prefix);
    } 

    // Set new TTL
    long long new_ttl = Get_Client_Query_TTL(client_tracking_group);
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

int Add_Event_To_Stream(RedisModuleCtx *ctx, const std::string client, const std::string event, const std::string key, const std::string value, const std::string queries, bool send_old_value ) { 

    if( Is_Client_Connected(client) == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Event_To_Stream skipping offline client : " + client);
        return REDISMODULE_OK;
    } else {
         LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Event_To_Stream adding for client:  " + client);
    }
    RedisModuleString *client_name = RedisModule_CreateString(ctx, client.c_str(), client.length());
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
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


int Trim_From_Stream(RedisModuleCtx *ctx, RedisModuleString *last_read_id, std::string client_name) {
    RedisModuleStreamID minid;
    if (RedisModule_StringToStreamID(last_read_id, &minid) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Trim_From_Stream:  Provided Stream ID is not valid.");
        return REDISMODULE_ERR;
    }

    minid.seq = minid.seq + 1; // We get last read so delete the last read too 

    RedisModuleString *client_name_r = RedisModule_CreateString(ctx, client_name.c_str(), client_name.length());
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name_r, REDISMODULE_WRITE);

    if (RedisModule_StreamTrimByID(stream_key, 0, &minid) < 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Trim_From_Stream:  Trim with given Stream ID failed.");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
