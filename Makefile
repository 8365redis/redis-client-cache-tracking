CC = g++
CPPFLAGS = -Wall -g -fPIC -lc -lm -Og -std=c++11 -Isrc
LDFLAGS = -shared
DEBUGFLAGS = -O0 -D _DEBUG
RELEASEFLAGS = -O2 -D NDEBUG -combine -fwhole-program

BINDIR = bin
SRCDIR = src

SOURCES = $(shell echo src/*.c)
HEADERS = $(shell echo src/*.h)
OBJECTS = $(SOURCES:.c=.o)



TARGET  = $(BINDIR)/cct.so
all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CPPFLAGS) $(DEBUGFLAGS) -shared -o  $(TARGET) $(OBJECTS)

clean:
	rm -rf  $(SRCDIR)/*.o $(BINDIR)/*.so 
	rm dump.rdb

load: 
	redis-stack-server --loadmodule $(BINDIR)/cct.so

test:
	pytest -rP
