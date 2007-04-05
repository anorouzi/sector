#ifndef __SECTOR_H__
#define __SECTOR_H__

#include "client.h"
#include <string>
#include <spe.h>
#include <pthread.h>

using namespace std;
using namespace cb;

struct STREAM
{
   string m_strDataFile;
   int64_t m_llSize;
   int32_t m_iUnitSize;
};

class SPEClient: public Client
{
friend class Process;

public:
   Process* createJob();
   int releaseJob(Process* proc);
};

class Process
{
friend class SPEClient;

public:
   Process();
   ~Process();

   int open(STREAM stream, string op, const char* param = NULL, const int& size = 0);
   int close();

   int run();
   int read(char*& data, int& size);

private:
   static void* run(void*);

private:
   vector<SPE> m_vSPE;

   struct Result
   {
      char* m_pcRes;
      int m_iSize;
   };
   vector<Result> m_vResult;

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   CGMP m_GMP;

   SPEClient* m_pSPEClient;
};

#endif
