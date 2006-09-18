#ifndef __GMP_H__
#define __GMP_H__

#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <message.h>
#include <prec.h>

#include <vector>
#include <string>
#include <map>
#include <set>
#include <list>
#include <queue>
using namespace std;


class CGMPMessage
{
public:
   CGMPMessage();
   ~CGMPMessage();

   int32_t& m_iType;		// 0 Data; 1 ACK
   int32_t& m_iSession;
   int32_t& m_iID;		// message ID
   int32_t& m_iInfo;		//

   char* m_pcData;
   int m_iLength;

   int32_t m_piHeader[4];

public:
   void pack(const char* data, const int& len, const int32_t& info = 0);
   void pack(const int32_t& type, const int32_t& info = 0);

public:
   static int32_t g_iSession;

private:
   static int32_t initSession();

   static int32_t g_iID;
   static pthread_mutex_t g_IDLock;

   static const int32_t g_iMaxID = 0xFFFFFFF;

   static const int m_iHdrSize = 16;
};

struct CMsgRecord
{
   char m_pcIP[64];
   int m_iPort;

   CGMPMessage* m_pMsg;

   timeval m_TimeStamp;
};

struct CFMsgRec
{
   bool operator()(const CMsgRecord* m1, const CMsgRecord* m2) const
   {
      if (strcmp(m1->m_pcIP, m2->m_pcIP) == 0)
      {
         if (m1->m_iPort == m2->m_iPort)
            return m1->m_pMsg->m_iID > m2->m_pMsg->m_iID;

         return (m1->m_iPort > m2->m_iPort);
      }
      
      return strcmp(m1->m_pcIP, m2->m_pcIP);
   }
};


class CGMP
{
public:
   CGMP();
   ~CGMP();

public:
   int init(const int& port = 0);
   int close();

public:
   int sendto(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable = true);
   int recvfrom(char* ip, int& port, int32_t& id, char* data, int& len);
   int recv(const int32_t& id, char* data, int& len);

private:
   int UDPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable = true);
   int TCPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len);

public:
   int sendto(const char* ip, const int& port, int32_t& id, const CUserMessage* msg);
   int recvfrom(char* ip, int& port, int32_t& id, CUserMessage* msg);
   int recv(const int32_t& id, CUserMessage* msg);
   int rpc(const char* ip, const int& port, CUserMessage* req, CUserMessage* res);

   int rtt(const char* ip, const int& port);

private:
   pthread_t m_SndThread;
   pthread_t m_RcvThread;
   pthread_t m_TCPRcvThread;
   static void* sndHandler(void*);
   static void* rcvHandler(void*);
   static void* tcpRcvHandler(void*);

   pthread_mutex_t m_SndQueueLock;
   pthread_cond_t m_SndQueueCond;
   pthread_mutex_t m_RcvQueueLock;
   pthread_cond_t m_RcvQueueCond;
   pthread_mutex_t m_ResQueueLock;
   pthread_cond_t m_ResQueueCond;
   pthread_mutex_t m_RTTLock;
   pthread_cond_t m_RTTCond;

private:
   int m_iPort;

   int m_iUDPSocket;
   int m_iTCPSocket;

   list<CMsgRecord*> m_lSndQueue;
   queue<CMsgRecord*> m_qRcvQueue;
   map<int32_t, CMsgRecord*> m_mResQueue;
   CPeerManagement m_PeerHistory;

   volatile bool m_bClosed;

private:
   static const int m_iMaxUDPMsgSize = 1456;
};

#endif
