CC = gcc
CFLAGS = -Wall -g -fPIC -lc -lm -Og -std=gnu99 -Isrc
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
	$(CC) $(CFLAGS) $(DEBUGFLAGS) -shared -o  $(TARGET) $(OBJECTS)

clean:
	rm -rf  $(SRCDIR)/*.o $(BINDIR)/*.so
