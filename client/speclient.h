#ifndef __SECTOR_H__
#define __SECTOR_H__

#include "client.h"
#include <string>
#include <pthread.h>
#include <udt.h>

using namespace std;

namespace cb
{

class Process
{
friend class Client;

public:
   Process();
   ~Process();

   int open(vector<string> stream, string op, const char* param = NULL, const int& size = 0);
   int close();

   int run();
   int checkProgress();
   int read(char*& data, int& size, string& file, int64_t& offset, int& rows, const bool& inorder = true, const bool& wait = true);

private:
   static void* run(void*);

private:
   string m_strOperator;
   string m_strParam;
   vector<string> m_vstrStream;

   struct DS
   {
      string m_strDataFile;
      int64_t m_llOffset;
      int64_t m_llSize;

      int m_iSPEID;

      int m_iStatus;		// 0: not started yet; 1: in progress; 2: done, result ready; 3: result read
      char* m_pcResult;
      int m_iResSize;
   };
   vector<DS> m_vDS;

   struct SPE
   {
      uint32_t m_uiID;
      string m_strIP;
      int m_iPort;

      DS* m_pDS;
      int m_iStatus;
      int m_iProgress;
      timeval m_LastUpdateTime;

      UDTSOCKET m_DataSock;
   };
   vector<SPE> m_vSPE;

   int m_iProgress;
   int m_iAvailRes;

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   int m_iMinUnitSize;
   int m_iMaxUnitSize;

   CGMP m_GMP;
};

}; // namespace cb

#endif
