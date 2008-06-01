C++ = g++

all: randwriter.so sort.so sorthash.so

randwriter.so: randwriter.cpp
	$(C++) -shared -fPIC -o $@ $^ -lstdc++

sort.so: sort.cpp
	$(C++) -shared -fPIC -o $@ $^ -lstdc++

sorthash.so: sorthash.cpp
	$(C++) -shared -fPIC -o $@ $^ -lstdc++

clean:
	rm -f randwriter.so sort.so sorthash.so
