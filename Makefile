GPP = g++
FLAGS = -O3 -g
FLAGS += -std=c++2a
FLAGS += -fopenmp
FLAGS += -DNTIMER
LDFLAGS = -lpthread
SRCS = benchmark.cpp hashmap.cpp

all: benchmark benchmark_debug

.PHONY: benchmark
benchmark:
	$(GPP) $(FLAGS) -o $@.out $(SRCS) $(LDFLAGS) -DNDEBUG

.PHONY: benchmark_debug
benchmark_debug:
	$(GPP) $(FLAGS) -o $@.out $(SRCS) -DTRACE=if\(1\) $(LDFLAGS)

clean:
	rm -f *.out
