# Use an appropriate base image for building the module
FROM gcc:latest as builder

# Install required packages
RUN apt-get update && apt-get install -y \
    make \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /usr/src/app

# Copy the repository files to the container
COPY . .

# Build the Redis module
RUN make DEBUG=1

# Stage 2: Create the runtime image
FROM redis/redis-stack:latest

# Install gdb in the runtime image
RUN apt-get update && apt-get install -y  \
    gdbserver \
    gdb  \
    make  \
    cmake && rm -rf /var/lib/apt/lists/*

# Copy the built module from the builder stage
COPY --from=builder /usr/src/app/bin/cct.so /usr/local/lib/redis/modules/cct.so
COPY --from=builder /usr/src/app/src /usr/src/app/src
COPY --from=builder /usr/src/app/include /usr/src/app/include

# Expose ports for gdbserver
EXPOSE 6379 1234

# Command to run Redis with the loaded module
CMD ["redis-stack-server", "--loadmodule", "/usr/local/lib/redis/modules/cct.so", "--protected-mode", "no"]
