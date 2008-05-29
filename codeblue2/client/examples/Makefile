C++ = g++

all: randwriter sort.so sorthash.so

randwriter: randwriter.cpp
	$(C++) $^ -o $@ -lstdc++

sort.so: sort.cpp
	$(C++) -shared -fPIC -o $@ $^ -lstdc++

sorthash.so: sorthash.cpp
	$(C++) -shared -fPIC -o $@ $^ -lstdc++

clean:
	rm -f randwriter sort.so sorthash.so
