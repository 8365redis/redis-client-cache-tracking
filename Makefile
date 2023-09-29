#!/bin/bash

SHELL := /bin/bash
VERSION := $(shell git describe --abbrev=0)

DEBUG ?= 0
ifeq ($(DEBUG), 1)
    CPPFLAGS = -Wall -g -fPIC -std=c++11 -I$(INCDIR) -D CCT_MODULE_VERSION=\"$(VERSION)\" -D _DEBUG -O0
else
    CPPFLAGS = -Wall -g -fPIC -std=c++11 -I$(INCDIR) -D CCT_MODULE_VERSION=\"$(VERSION)\" -D NDEBUG -O3
endif

CC = g++
LDFLAGS = -static-libstdc++ -shared -o
 
BINDIR = bin
SRCDIR = src
INCDIR = include

SOURCES = $(shell echo src/*.cpp)
HEADERS = $(shell echo include/*.h)
OBJECTS = $(SOURCES:.cpp=.o)

TARGET  = $(BINDIR)/cct.so

all: $(TARGET)

$(TARGET) : $(OBJECTS)
	$(CC) $(CPPFLAGS) $(LDFLAGS) $(TARGET) $(OBJECTS)
	rm -rf  $(SRCDIR)/*.o

.PHONY: clean load test perf_test

clean:
	rm -rf  $(SRCDIR)/*.o $(BINDIR)/*.so 
	rm -f dump.rdb

load: 
	redis-stack-server --loadmodule $(BINDIR)/cct.so

test:
	pytest -rP

perf_test:
	pytest -rP python/cct_perf_test.py
