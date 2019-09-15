CXX?=g++9.2
CFLAGS=-std=c++11 -c -g -Wall
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CXX) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
