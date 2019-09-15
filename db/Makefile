CXX?=g++9.2
CXXFLAGS+=-std=c++11 -c -g -Wall -fgnu-tm
INCLUDES=-I../
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

