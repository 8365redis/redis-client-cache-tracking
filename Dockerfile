FROM gcc:latest as builder

RUN apt-get update && apt-get install -y \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY . .

RUN make DEBUG=1

FROM redis/redis-stack:latest

RUN apt-get update && apt-get install -y  \
    gdbserver \
    gdb  \
    make  \
    g++ \
    cmake && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/bin/cct.so /usr/local/lib/redis/modules/cct.so
COPY --from=builder /usr/src/app/src /usr/src/app/src
COPY --from=builder /usr/src/app/include /usr/src/app/include

EXPOSE 6379 1234

CMD ["redis-stack-server", "--loadmodule", "/usr/local/lib/redis/modules/cct.so", "--protected-mode", "no"]
