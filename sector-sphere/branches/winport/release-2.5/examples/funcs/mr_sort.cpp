#ifndef WIN32
    #include <sys/time.h>
    #include <unistd.h>
#endif
#include <iostream>
#include <stdint.h>

#include "sphere.h"

#ifdef WIN32
    #ifdef MR_SORT_EXPORTS
        #define MR_SORT_API __declspec(dllexport)
    #else
        #define MR_SORT_API __declspec(dllimport)
    #endif
#else
    #define MR_SORT_API
#endif


using namespace std;

extern "C"
{
// use extern "C" to let g++ use the C style function naming
// otherwise the function would not be located in the dynamic library

struct Key
{
   uint32_t v1;
   uint32_t v2;
   uint16_t v3;
};

//For Terasort, the Map function does nothing
//int mr_sort_map(const SInput* input, SOutput* output, SFile* file)
//{
//}

int MR_SORT_API mr_sort_partition(const char* record, int size, void* param, int psize)
{
   Key* k = (Key*)record;
   int n = *(int*)param;
   // hash k into a value in [0, 2^n -1), n < 32
   return (k->v1 >> (32 - n));
}

int MR_SORT_API mr_sort_compare(const char* r1, int s1, const char* r2, int s2)
{
   Key* k1 = (Key*)r1;
   Key* k2 = (Key*)r2;

   if (k1->v1 > k2->v1)
      return 1;
   if (k1->v1 < k2->v1)
      return -1;

   if (k1->v2 > k2->v2)
      return 1;
   if (k1->v2 < k2->v2)
      return -1;

   if (k1->v3 > k2->v3)
      return 1;
   if (k1->v3 < k2->v3)
      return -1;

   return 0;
}

// for TeraSort, the reduce function does nothing
//int mr_sort_reduce(const SInput* input, SOutput* output, SFile* file)
//{
//}


}
