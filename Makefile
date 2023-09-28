#!/bin/bash

SHELL := /bin/bash

DESCRIBE           := $(shell git describe --match "v*" --always --tags)
DESCRIBE_PARTS     := $(subst -, ,$(DESCRIBE))

VERSION_TAG        := $(word 1,$(DESCRIBE_PARTS))
COMMITS_SINCE_TAG  := $(word 2,$(DESCRIBE_PARTS))

VERSION            := $(subst v,,$(VERSION_TAG))
VERSION_PARTS      := $(subst ., ,$(VERSION))

MAJOR              := $(word 1,$(VERSION_PARTS))
MINOR              := $(word 2,$(VERSION_PARTS))
MICRO              := $(word 3,$(VERSION_PARTS))

NEXT_MAJOR         := $(shell echo $(($(MAJOR)+1)))
NEXT_MINOR         := $(shell echo $(($(MINOR)+1)))
NEXT_MICRO         := $(shell echo $(($(MICRO)+1)))

ifeq ($(strip $(COMMITS_SINCE_TAG)),)
CURRENT_VERSION_MICRO := $(MAJOR).$(MINOR).$(MICRO)
CURRENT_VERSION_MINOR := $(CURRENT_VERSION_MICRO)
CURRENT_VERSION_MAJOR := $(CURRENT_VERSION_MICRO)
else
CURRENT_VERSION_MICRO := $(MAJOR).$(MINOR).$(NEXT_MICRO)
CURRENT_VERSION_MINOR := $(MAJOR).$(NEXT_MINOR).0
CURRENT_VERSION_MAJOR := $(NEXT_MAJOR).0.0
endif

DATE                = $(shell date +'%d.%m.%Y')
TIME                = $(shell date +'%H:%M:%S')
COMMIT             := $(shell git rev-parse HEAD)
AUTHOR             := $(firstword $(subst @, ,$(shell git show --format="%aE" $(COMMIT))))
BRANCH_NAME        := $(shell git rev-parse --abbrev-ref HEAD)



CC = g++
CPPFLAGS = -Wall -g -fPIC -lc -lm -Og -std=c++11 -I$(INCDIR) -DCCT_MODULE_VERSION=\"$(CURRENT_VERSION_MAJOR).$(CURRENT_VERSION_MINOR).$(CURRENT_VERSION_MICRO)\"
LDFLAGS = -static-libstdc++ -shared
DEBUGFLAGS = -O0 -D _DEBUG
RELEASEFLAGS = -O2 -D NDEBUG -combine -fwhole-program
 
BINDIR = bin
SRCDIR = src
INCDIR = include

SOURCES = $(shell echo src/*.cpp)
HEADERS = $(shell echo include/*.h)
OBJECTS = $(SOURCES:.cpp=.o)

TARGET  = $(BINDIR)/cct.so
all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CPPFLAGS) $(DEBUGFLAGS) $(LDFLAGS) -o  $(TARGET) $(OBJECTS)
	rm -rf  $(SRCDIR)/*.o

clean:
	rm -rf  $(SRCDIR)/*.o $(BINDIR)/*.so 
	rm -f dump.rdb

load: 
	redis-stack-server --loadmodule $(BINDIR)/cct.so

test:
	pytest -rP

perf_test:
	pytest -rP python/cct_perf_test.py
