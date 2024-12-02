#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cstdlib>
#include <chrono>
#include <hiredis/hiredis.h>
#include <json/json.hpp>

using json = nlohmann::json;

const std::string INDEX_NAME = "benchmark_index";
const std::string COMMON_VALUE = "common_value";

enum Mode {
    FT_SEARCH = 0,
    CCT2_FT_SEARCH = 1,
    TRACE_EXECUTE = 2
};

std::chrono::steady_clock::time_point getStartTime() {
    return std::chrono::steady_clock::now();
}

double getDeltaTime(std::chrono::steady_clock::time_point start) {
    const auto end = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

void printResponse(redisReply *reply) {
    if(reply->type == REDIS_REPLY_ARRAY){
        for (std::size_t i = 0; i < reply->elements; i++) {
            if(reply->element[i]->type == REDIS_REPLY_ARRAY) {
                printResponse(reply->element[i]);
            } else if(reply->element[i]->type == REDIS_REPLY_STRING){
                std::cout << reply->element[i]->str << std::endl;
            } else if(reply->element[i]->type == REDIS_REPLY_INTEGER){
                std::cout << reply->element[i]->integer << std::endl;
            } else if(reply->element[i]->type == REDIS_REPLY_DOUBLE){
                std::cout << reply->element[i]->dval << std::endl;
            }
        }
    } else if(reply->type == REDIS_REPLY_STRING){
        std::cout << reply->str << std::endl;
    } else if(reply->type == REDIS_REPLY_INTEGER){
        std::cout << reply->integer << std::endl;
    } else if(reply->type == REDIS_REPLY_DOUBLE){
        std::cout << reply->dval << std::endl;
    } else if(reply->type == REDIS_REPLY_ERROR){
        std::cout << reply->str << std::endl;
    }
}

void flushDatabase(redisContext* context) {
    redisReply* reply = (redisReply*)redisCommand(context, "FLUSHALL");
    if (reply == nullptr) {
        std::cerr << "Error flushing database: " << context->errstr << std::endl;
        exit(1);
    }
    std::cout << "Database flushed: " << reply->str << std::endl;
    freeReplyObject(reply);
}


void createIndex(redisContext* context, int attributeCount) {
    std::ostringstream command;
    command << "FT.CREATE " << INDEX_NAME << " ON JSON PREFIX 1 doc: SCHEMA ";

    for (int i = 1; i <= attributeCount; ++i) {
        command << "$.attribute" << i << " AS attribute" << i << " TAG ";
    }

    //std::cout << "Create command: " << command.str() << std::endl;
    redisReply* reply = (redisReply*)redisCommand(context, command.str().c_str());
    if (reply == nullptr) {
        std::cerr << "Error creating index: " << context->errstr << std::endl;
        exit(1);
    }
    std::cout << "Index created: " << reply->str << std::endl;
    freeReplyObject(reply);
}

void addData(redisContext* context, int keyCount, int attributeCount, int resultCount) {
    for (int i = 1; i <= keyCount; ++i) {
        std::ostringstream key;
        key << "doc:" << i;

        json jsonData;
        for (int j = 1; j <= attributeCount; ++j) {
            if (i <= resultCount) {
                jsonData["attribute" + std::to_string(j)] = COMMON_VALUE;
            } else {
                jsonData["attribute" + std::to_string(j)] = "value" + std::to_string(j) + "_" + std::to_string(i);
            }
        }

        std::ostringstream command;
        command << "JSON.SET " << key.str() << " $ " << jsonData.dump();

        //std::cout << "Add command: " << command.str() << std::endl;
        //auto start = getStartTime();
        redisReply* reply = (redisReply*)redisCommand(context, command.str().c_str());
        //auto delta = getDeltaTime(start);
        //std::cout << "Add time: " << delta << " ns" << std::endl;
        if (reply == nullptr) {
            std::cerr << "Error adding data: " << context->errstr << std::endl;
            exit(1);
        }
        //std::cout << "Added: " << reply->str << std::endl;
        freeReplyObject(reply);
    }
    std::cout << "Added " << keyCount << " keys." << std::endl;
}

void searchIndex(redisContext* context, Mode mode) {
    std::ostringstream command;
    if(mode == FT_SEARCH){
        command << "FT.SEARCH " << INDEX_NAME << " @attribute1:{" << COMMON_VALUE << "}";
    } else if(mode == CCT2_FT_SEARCH){
        command << "CCT2.FT.SEARCH " << INDEX_NAME << " @attribute1:{" << COMMON_VALUE << "}";
    } else if(mode == TRACE_EXECUTE){
        command << "TRACE.EXECUTE BENCHMARK_CLIENT CMD CCT2.FT.SEARCH " << INDEX_NAME  << " @attribute1:{" << COMMON_VALUE << "}";
    }


    if (mode == TRACE_EXECUTE || mode == CCT2_FT_SEARCH){
        std::ostringstream register_command;
        if (mode == CCT2_FT_SEARCH){
            register_command << "CCT2.REGISTER BENCHMARK_CLIENT";
        } else {
            register_command << "TRACE.EXECUTE BENCHMARK_CLIENT CMD CCT2.REGISTER BENCHMARK_CLIENT";
        }
        auto start = getStartTime();
        redisReply* reply = (redisReply*)redisCommand(context, register_command.str().c_str());
        auto delta = getDeltaTime(start);
        std::cout << "Register time: " << delta << " ns" << std::endl;
    }

    //std::cout << "Search command: " << command.str() << std::endl;
    auto start = getStartTime();
    redisReply* reply = (redisReply*)redisCommand(context, command.str().c_str());
    //printResponse(reply);
    auto delta = getDeltaTime(start);
    std::cout << "Search time: " << delta << " ns" << std::endl;
    if (reply == nullptr) {
        std::cerr << "Error searching index: " << context->errstr << std::endl;
        exit(1);
    }

    //printResponse(reply);
    if(reply->element != nullptr && reply->element[0] != nullptr && reply->element[0]->type == REDIS_REPLY_INTEGER){
        std::cout << "Search results: " << reply->element[0]->integer << " results found." << std::endl;
    } else if(reply->element != nullptr && reply->element[0] != nullptr){
        std::cerr << "Unexpected reply type: " << reply->element[0]->type << std::endl;
        exit(1);
    }else{
        std::cerr << "Call to " << command.str() << " returned an error." << std::endl;
        exit(1);
    }

    freeReplyObject(reply);
}


int main(int argc, char* argv[]) {
    // Default parameters
    int mode = 0;
    std::string redisIp = "127.0.0.1";
    int redisPort = 6379;
    int keyCount = 10;
    int resultCount = 5;
    int attributeCount = 3;

    // Parse command line arguments
    if (argc > 1) {
        mode = std::atoi(argv[1]);
    }
    if (argc > 2) {
        redisIp = argv[2];
    }
    if (argc > 3) {
        redisPort = std::atoi(argv[3]);
    }
    if (argc > 4) {
        keyCount = std::atoi(argv[4]);
    }
    if (argc > 5) {
        resultCount = std::atoi(argv[5]);
    }
    if (argc > 6) {
        attributeCount = std::atoi(argv[6]);
    }

    // Display parameters
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "Using the following parameters:" << std::endl;
    std::cout << "Mode: " << (mode == FT_SEARCH ? "FT_SEARCH" : (mode == CCT2_FT_SEARCH ? "CCT2_FT_SEARCH" : "TRACE_EXECUTE")) << std::endl; 
    std::cout << "Redis IP: " << redisIp << std::endl;
    std::cout << "Redis Port: " << redisPort << std::endl;
    std::cout << "Number of Keys to Add: " << keyCount << std::endl;
    std::cout << "Number of Results to Return: " << resultCount << std::endl;
    std::cout << "Number of Attributes per Key: " << attributeCount << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    // Connect to Redis
    auto start = getStartTime();
    redisContext* context = redisConnect(redisIp.c_str(), redisPort);
    auto delta = getDeltaTime(start);
    std::cout << "Connection time: " << delta << " ns" << std::endl;
    if (context == nullptr || context->err) {
        if (context) {
            std::cerr << "Error: " << context->errstr << std::endl;
            redisFree(context);
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
        return 1;
    }

    // Flush database, create index, add data, and search
    flushDatabase(context);
    createIndex(context, attributeCount);
    addData(context, keyCount, attributeCount, resultCount);
    searchIndex(context, (Mode)mode);

    // Clean up
    redisFree(context);
    return 0;
}
