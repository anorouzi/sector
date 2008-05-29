#include <iostream>
#include <fstream>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdint.h>

using namespace std;

// 10 byte key, 90 byte value

struct Key
{
   uint32_t k1;
   uint32_t k2;
   uint16_t k3;
};

void keyinit();
void keygen(char* key);

int main(int argc, char** argv)
{
   ofstream ofs;
   ofs.open(argv[1], ios::binary);

   char record[100];
   
   keyinit();

   //10GB = 100 * 100000000
   for (int i = 0; i < 1000000; ++ i)
   {
      keygen(record);
      ofs.write(record, 100);
   }

   ofs.close();

   string ifile = string(argv[1]) + ".idx";
   ofstream idx(ifile.c_str(), ios::binary);

   for (int i = 0; i < 1000001; ++ i)
   {
      long long int d = i * 100;
      idx.write((char*)&d, 8);
   }

   idx.close();

   return 0;
}

void keyinit()
{
   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);
}

void keygen(char* key)
{
   int r = rand();
   *(int*)key = r;
   r = rand();
   *(int*)(key + 4) = r;
   r = rand();
   *(int*)(key + 8) = r;
}
