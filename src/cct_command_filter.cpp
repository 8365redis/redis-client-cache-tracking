#include <string.h>

#include "cct_command_filter.h"
#include "logger.h"
#include "cct_index_tracker.h"


const char* FT_CREATE = "FT.CREATE";
const int FT_CREATE_SIZE = 9;

void Command_Filter_Callback(RedisModuleCommandFilterCtx *filter) {

    size_t command_len;
    const RedisModuleString *command = RedisModule_CommandFilterArgGet(filter, 0);
    const char *command_str = RedisModule_StringPtrLen(command, &command_len);
    //printf("Command %s\n", command_str);
    // quick fail 
    if (*command_str != 'F' && *command_str != 'f') {
        return;
    }

    if (command_len == FT_CREATE_SIZE && !strcasecmp(command_str, FT_CREATE)) {
        auto& index_manager = Redis_Index_Manager::Instance();
        index_manager.Set_Index_Change(true);
        //printf("FT.CREATE detected %s\n", command_str);
    }

}