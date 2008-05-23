#include <sys/time.h>
#include <unistd.h>
#include <iostream>
#include <stdint.h>
using namespace std;

extern "C"
{

// unit: input data stream
// rows: number of rows
// index: input rows index
// result: result data stream
// rsize: result size
// rrows: number of output rows
// rindex: output rows index
// param: parameters
// psize: size of the parameter

struct Key
{
   uint32_t v1;
   uint32_t v2;
   uint16_t v3;
};

// hash k into a value in [0, 2^n -1), n < 32
int hash(const Key* k, const int& n)
{
   return (k->v1 >> (32 - n));
}

int sorthash(const char* unit, const int& rows, const int64_t& index, char* result, int& rsize, int& rrows, int64_t* rindex, int& bid, const char* param, const int& psize)
{
   memcpy(result, unit, 100);
   *rindex = 0;
   *(rindex + 1) = 100;

   rsize = 100;
   rrows = 1;

   bid = hash((Key*)unit, *(int*)param);

   return 0;
}

}
