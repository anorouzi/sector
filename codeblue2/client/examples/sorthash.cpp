#include <sys/time.h>
#include <unistd.h>
#include <iostream>
#include <stdint.h>
#include "../../common/sphere.h"

using namespace std;

extern "C"
{

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

int sorthash(const SInput* input, SOutput* output, SFile* file)
{
   memcpy(output->m_pcResult, input->m_pcUnit, 100);
   *(output->m_pllIndex) = 0;
   *(output->m_pllIndex + 1) = 100;
   output->m_iResSize = 100;
   output->m_iRows = 1;
   *output->m_piBucketID = hash((Key*)input->m_pcUnit, *(int*)input->m_pcParam);

   return 0;
}

}
