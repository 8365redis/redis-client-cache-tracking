# redis-client-cache-tracking
Redis module for client cache tracking

## Development Environment 

* IDE : [Visual Studio Code with WSL](https://code.visualstudio.com/docs/cpp/config-wsl) 
* WSL : 5.15.90.1-microsoft-standard-WSL2 (Ubuntu Ubuntu 22.04.2 LTS)
* Redis server v=7.0.12 sha=00000000:0 malloc=jemalloc-5.2.1 bits=64 (Latest Stable Package)
* Redis Modules :
    * [Redis Search] (https://github.com/RediSearch/RediSearch)
    * [Redis JSON] (https://github.com/RedisJSON/RedisJSON)

## Build

After preparing the build environment just run : 

```
make
```

## Load

```
redis-server --loadmodule ./bin/cct.so
```

or 

```
make load
```

## Commands

TBD
