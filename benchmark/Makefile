CXX = g++
CXXFLAGS = -Wall -std=c++11 -I$(INCDIR) 
INCDIR = include

LIBSDIR = ./libs
LDFLAGS = -L$(LIBSDIR)
LIBS = -lhiredis

TARGET = redis_benchmark
SOURCES = $(shell echo src/*.cpp)

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SOURCES) $(LIBS) $(LDFLAGS)

clean:
	rm -f $(TARGET)