#include <sys/time.h>
#include <time.h>
#include <prec.h>
#include <iostream>

using namespace std;

CPeerManagement::CPeerManagement()
{
   pthread_mutex_init(&m_PeerRecLock, NULL);
}

CPeerManagement::~CPeerManagement()
{
   pthread_mutex_destroy(&m_PeerRecLock);
}

void CPeerManagement::insert(const string& ip, const int& port, const int& session, const int32_t& id, const int& rtt)
{
   pthread_mutex_lock(&m_PeerRecLock);

   CPeerRecord* pr = new CPeerRecord;
   pr->m_strIP = ip;
   pr->m_iPort = port;
   pr->m_iSession = session;

   set<CPeerRecord*, CFPeerRec>::iterator i = m_sPeerRec.find(pr);

   if (i != m_sPeerRec.end())
   {
      if (id > 0)
         (*i)->m_iID = id;

      if (rtt > 0)
      {
         if (-1 == (*i)->m_iRTT )
            (*i)->m_iRTT = rtt;
         else
            (*i)->m_iRTT = ((*i)->m_iRTT * 7 + rtt) >> 3;
      }

      gettimeofday(&((*i)->m_TimeStamp), 0);

      delete pr;
   }
   else
   {
      if (id > 0)
         pr->m_iID = id;
      else
         pr->m_iID = -1;

      if (rtt > 0)
         pr->m_iRTT = rtt;
      else
         pr->m_iRTT = -1;

      gettimeofday(&(pr->m_TimeStamp), 0);

      m_sPeerRec.insert(pr);
      m_sPeerRecByTS.insert(pr);
      m_sPeerRecByIP.insert(pr);

      if (m_sPeerRecByTS.size() > m_uiRecLimit)
      {
         // delete first one
         set<CPeerRecord*, CFPeerRecByTS>::iterator j = m_sPeerRecByTS.begin();

         CPeerRecord* t = *j;
         m_sPeerRec.erase(t);
         pair<set<CPeerRecord*, CFPeerRecByIP>::iterator, set<CPeerRecord*, CFPeerRecByIP>::iterator> p = m_sPeerRecByIP.equal_range(t);
         for (set<CPeerRecord*, CFPeerRecByIP>::iterator k = p.first; k != p.second; k ++)
         {
            if ((*k)->m_iPort == t->m_iPort)
            {
               m_sPeerRecByIP.erase(k);
            }
         }
         delete t;
         m_sPeerRecByTS.erase(j);
      }
   }

   pthread_mutex_unlock(&m_PeerRecLock);
}

int CPeerManagement::getRTT(const string& ip)
{
   pair<set<CPeerRecord*, CFPeerRecByIP>::iterator, set<CPeerRecord*, CFPeerRecByIP>::iterator> p;

   CPeerRecord pr;
   pr.m_strIP = ip;

   p = m_sPeerRecByIP.equal_range(&pr);

   for (set<CPeerRecord*, CFPeerRecByIP>::iterator i = p.first; i != p.second; i ++)
   {
      if ((*i)->m_iRTT > 0)
         return (*i)->m_iRTT;
   }

   return -1;
}

int CPeerManagement::getLastID(const string& ip, const int& port, const int& session)
{
   CPeerRecord pr;
   pr.m_strIP = ip;
   pr.m_iPort = port;
   pr.m_iSession = session;

   set<CPeerRecord*, CFPeerRec>::iterator i = m_sPeerRec.find(&pr);

   if (i == m_sPeerRec.end())
      return -1;
   else
      return (*i)->m_iID;
}
