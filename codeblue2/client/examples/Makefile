C++ = g++
CCFLAGS = -shared -fPIC -g

all: randwriter.so sort.so sorthash.so wordbucket.so

randwriter.so: randwriter.cpp
	$(C++) $(CCFLAGS) -o $@ $^ -lstdc++

sort.so: sort.cpp
	$(C++) $(CCFLAGS) -o $@ $^ -lstdc++

sorthash.so: sorthash.cpp
	$(C++) $(CCFLAGS) -o $@ $^ -lstdc++

wordbucket.so: wordbucket.cpp
	$(C++) $(CCFLAGS) -o $@ $^ -lstdc++

clean:
	rm -f randwriter.so sort.so sorthash.so wordbucket.so
