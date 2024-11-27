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

void searchIndex(redisContext* context) {
    std::ostringstream command;
    command << "FT.SEARCH " << INDEX_NAME << " @attribute1:{" << COMMON_VALUE << "}";

    //std::cout << "Search command: " << command.str() << std::endl;
    auto start = getStartTime();
    redisReply* reply = (redisReply*)redisCommand(context, command.str().c_str());
    auto delta = getDeltaTime(start);
    std::cout << "Search time: " << delta << " ns" << std::endl;
    if (reply == nullptr) {
        std::cerr << "Error searching index: " << context->errstr << std::endl;
        exit(1);
    }

    //printResponse(reply);
    if(reply->element != nullptr && reply->element != nullptr && reply->element[0]->type == REDIS_REPLY_INTEGER){
        std::cout << "Search results: " << reply->element[0]->integer << " results found." << std::endl;
    } else {
        std::cerr << "Unexpected reply type: " << reply->element[0]->type << std::endl;
        exit(1);
    }

    freeReplyObject(reply);
}


int main(int argc, char* argv[]) {
    // Default parameters
    std::string redisIp = "127.0.0.1";
    int redisPort = 6379;
    int keyCount = 10;
    int resultCount = 5;
    int attributeCount = 3;

    // Parse command line arguments
    if (argc > 1) {
        redisIp = argv[1];
    }
    if (argc > 2) {
        redisPort = std::atoi(argv[2]);
    }
    if (argc > 3) {
        keyCount = std::atoi(argv[3]);
    }
    if (argc > 4) {
        resultCount = std::atoi(argv[4]);
    }
    if (argc > 5) {
        attributeCount = std::atoi(argv[5]);
    }

    // Display parameters
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "Using the following parameters:" << std::endl;
    std::cout << "Redis IP: " << redisIp << std::endl;
    std::cout << "Redis Port: " << redisPort << std::endl;
    std::cout << "Number of Keys to Add: " << keyCount << std::endl;
    std::cout << "Number of Results to Return: " << resultCount << std::endl;
    std::cout << "Number of Attributes per Key: " << attributeCount << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    // Connect to Redis
    redisContext* context = redisConnect(redisIp.c_str(), redisPort);
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
    searchIndex(context);

    // Clean up
    redisFree(context);
    return 0;
}
