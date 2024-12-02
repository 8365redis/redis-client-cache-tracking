# Redis JSON Benchmark Tool

This program is a C++ benchmarking tool designed to evaluate the performance of Redis and RedisJSON queries. It performs a series of operations including creating indexes, adding data, and searching the Redis database, while measuring the time taken for these operations. The tool provides a simple way to test the performance of different Redis and RedisJSON/Search versions.

[!WARNING]
> The program will FLUSHALL the current database before starting the benchmark.

## Features

- **Redis Integration:** Connects to a Redis database, flushing the current data.
- **Index Creation:** Creates an index on the JSON documents in Redis.
- **Data Insertion:** Adds a specified number of keys, each containing multiple attributes.
- **Search Operation:** Searches the index for specific values and prints the search results and time taken.

## Prerequisites

- **Redis:** You need to have Redis installed. This tool specifically uses the RedisJSON and RediSearch modules for working with JSON data.
- **Redis Modules:** Ensure you have RedisJSON/RediSearch installed in your Redis setup to support JSON commands.
- **Internal Modules:** Ensure you have the internal modules installed in your Redis setup to support CCT2 and TRACE_EXECUTE commands.
- **Dependencies:**
  - [Hiredis](https://github.com/redis/hiredis): Redis client for C.
  - [nlohmann/json](https://github.com/nlohmann/json): JSON library for C++.(header only)

## Building the Project

This project requires a C++ compiler with support for C++11 or later. You will also need `hiredis` and `nlohmann/json` libraries installed on your system.

### Compilation Example

To compile the code, use the following command:

```sh
g++ -o redis_benchmark redis_benchmark.cpp -lhiredis -ljsoncpp
```

## Running the Program

[!WARNING]
> The program will FLUSHALL the current database before starting the benchmark.

### Syntax

```sh
./redis_benchmark [REDIS_IP] [REDIS_PORT] [KEY_COUNT] [RESULT_COUNT] [ATTRIBUTE_COUNT]
```

### Parameters

- **MODE (optional):** The mode of the benchmark. 0 for FT_SEARCH, 1 for CCT2_FT_SEARCH, 2 for TRACE_EXECUTE (default is `0`).
- **REDIS_IP (optional):** The IP address of your Redis server (default is `127.0.0.1`).
- **REDIS_PORT (optional):** The port of your Redis server (default is `6379`).
- **KEY_COUNT (optional):** Number of keys to add (default is `10`).
- **RESULT_COUNT (optional):** Number of results expected in search (default is `5`).
- **ATTRIBUTE_COUNT (optional):** Number of attributes per key (default is `3`).

### Example Run

[!NOTE]
> The program will FLUSHALL the current database before starting the benchmark.

To run the benchmark with default parameters:

```sh
./redis_benchmark
```

To specify Redis server IP and port, as well as other parameters:

```sh
./redis_benchmark 0
```

```sh
./redis_benchmark 0 192.168.0.10 6379 100 50 5
```

## Output

The program performs the following actions and prints output for each:

- **Flush Database:** Clears all keys in the Redis database.
- **Create Index:** Creates an index on JSON keys with the specified number of attributes.
- **Add Data:** Adds JSON data to Redis with the specified number of keys and attributes.
- **Search Index:** Searches the index based on a common attribute value, displaying the time taken and the number of results found.


