#ifndef __STORE_H__
#define __STORE_H__

#include <routing.h>
#include <gmp.h>
#include <udt.h>
#include <log.h>
#include <index.h>


class CStore
{
public:
   CStore(const string& ip);
   ~CStore();

public:
   int init(char* ip = NULL, int port = 0);
   int run();

private:
   struct Param1
   {
      CStore* s;
      char ip[64];
      int port;
      int32_t id;
      CCBMsg* msg;
   };

   struct Param2
   {
      CStore* s;
      string fn;	// filename

      UDTSOCKET u;
      int t;		// TCP socket
      int c;		// connection type

      int m;		// file access mode
   };   

   static void* run(void* s);

   static void* process(void* p);

   static void* remote(void* p);

private:
   void updateOutLink();
   void updateInLink();

   int initLocalFile();

   void updateNameIndex(int& next);

private:
   int checkIndexLoc(const unsigned int& id);

private:
   CRouting m_Router;

   CGMP m_GMP;

   CIndex m_LocalFile;
   CIndex m_RemoteFile;

   CNameIndex m_NameIndex;

   string m_strLocalHost;
   int m_iLocalPort;

   static const int m_iKeySpace = 16;

   string m_strHomeDir;

   CAccessLog m_AccessLog;
   CPerfLog m_PerfLog;
};


#endif
