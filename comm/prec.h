#ifndef __PEER_RECORD_H__
#define __PEER_RECORD_H__

#include <pthread.h>
#include <set>
#include <string>
using namespace std;


struct CPeerRecord
{
   string m_strIP;
   int m_iPort;
   int m_iSession;
   int32_t m_iID;
   timeval m_TimeStamp;
   int m_iRTT;
};

struct CFPeerRec
{
   bool operator()(const CPeerRecord* p1, const CPeerRecord* p2) const
   {
      if (p1->m_strIP == p2->m_strIP)
      {
         if (p1->m_iPort == p2->m_iPort)
            return (p1->m_iSession > p2->m_iSession);

         return (p1->m_iPort > p2->m_iPort);
      }

      return (p1->m_strIP > p2->m_strIP);
   }
};

struct CFPeerRecByIP
{
   bool operator()(const CPeerRecord* p1, const CPeerRecord* p2) const
   {
      return (p1->m_strIP > p2->m_strIP);
   }
};

struct CFPeerRecByTS
{
   bool operator()(const CPeerRecord* p1, const CPeerRecord* p2) const
   {
      if (p1->m_TimeStamp.tv_sec == p2->m_TimeStamp.tv_sec)
         return (p1->m_TimeStamp.tv_usec > p2->m_TimeStamp.tv_usec);

      return (p1->m_TimeStamp.tv_sec > p2->m_TimeStamp.tv_sec);
   }
};

class CPeerManagement
{
public:
   CPeerManagement();
   ~CPeerManagement();

public:
   void insert(const string& ip, const int& port, const int& session, const int32_t& id = -1, const int& rtt = -1);
   int getRTT(const string& ip);
   int getLastID(const string& ip, const int& port, const int& session);

private:
   set<CPeerRecord*, CFPeerRec> m_sPeerRec;
   set<CPeerRecord*, CFPeerRecByTS> m_sPeerRecByTS;
   multiset<CPeerRecord*, CFPeerRecByIP> m_sPeerRecByIP;

   pthread_mutex_t m_PeerRecLock;

private:
   static const int m_iRecLimit = 500;   
};


#endif
