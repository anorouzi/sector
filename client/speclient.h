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
   int read(char*& data, int& size, string& file, int64_t& offset, int& rows, const bool& inorder);

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

      char* m_pResult;
      int m_iResSize;
   };
   vector<DS> m_vDS;

   struct SPE
   {
      uint32_t m_uiID;
      Node m_Loc;
      DS* m_pDS;
      int m_iStatus;
      int m_iProgress;

      UDTSOCKET m_DataSock;
   };
   vector<SPE> m_vSPE;

   int m_iProgress;

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   int m_iMinUnitSize;
   int m_iMaxUnitSize;

   CGMP m_GMP;
};

}; // namespace cb

#endif
