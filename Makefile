CC = g++
CPPFLAGS = -Wall -g -fPIC -lc -lm -Og -std=c++11 -I$(INCDIR)
LDFLAGS = -shared
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
	$(CC) $(CPPFLAGS) $(DEBUGFLAGS) -shared -o  $(TARGET) $(OBJECTS)
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
