/*****************************************************************************
Copyright (c) 2001 - 2009, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 05/25/2009
*****************************************************************************/

#ifndef WIN32
   #include <sys/time.h>
   #include <time.h>
#else
   #include <windows.h>
#endif

#include <common.h>
#include <prec.h>

using namespace std;

CPeerManagement::CPeerManagement()
{
   #ifndef WIN32
      pthread_mutex_init(&m_PeerRecLock, NULL);
   #else
      m_PeerRecLock = CreateMutex(NULL, false, NULL);
   #endif
}

CPeerManagement::~CPeerManagement()
{
   #ifndef WIN32
      pthread_mutex_destroy(&m_PeerRecLock);
   #else
      CloseHandle(m_PeerRecLock);
   #endif
}

void CPeerManagement::insert(const string& ip, const int& port, const int& session, const int32_t& id, const int& rtt, const int& fw)
{
   CGuard recguard(m_PeerRecLock);

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

      (*i)->m_llTimeStamp = CTimer::getTime();

      (*i)->m_iFlowWindow = fw;

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

      pr->m_llTimeStamp = CTimer::getTime();

      pr->m_iFlowWindow = fw;

      m_sPeerRec.insert(pr);
      m_sPeerRecByTS.insert(pr);
      m_sPeerRecByIP.insert(pr);

      if (m_sPeerRecByTS.size() > m_uiRecLimit)
      {
         // delete first one
         set<CPeerRecord*, CFPeerRecByTS>::iterator j = m_sPeerRecByTS.begin();

         CPeerRecord* t = *j;
         m_sPeerRec.erase(t);

         pair<multiset<CPeerRecord*, CFPeerRecByIP>::iterator, multiset<CPeerRecord*, CFPeerRecByIP>::iterator> p;
         p = m_sPeerRecByIP.equal_range(t);
         for (multiset<CPeerRecord*, CFPeerRecByIP>::iterator k = p.first; k != p.second; k ++)
         {
            if ((*k)->m_iPort == t->m_iPort)
            {
               m_sPeerRecByIP.erase(k);
               break;
            }
         }

         m_sPeerRecByTS.erase(t);

         delete t;
      }
   }
}

int CPeerManagement::getRTT(const string& ip)
{
   pair<multiset<CPeerRecord*, CFPeerRecByIP>::iterator, multiset<CPeerRecord*, CFPeerRecByIP>::iterator> p;

   CPeerRecord pr;
   pr.m_strIP = ip;
   int rtt = -1;

   CGuard recguard(m_PeerRecLock);

   p = m_sPeerRecByIP.equal_range(&pr);

   for (multiset<CPeerRecord*, CFPeerRecByIP>::iterator i = p.first; i != p.second; i ++)
   {
      if ((*i)->m_iRTT > 0)
      {
         rtt = (*i)->m_iRTT;
         break;
      }
   }

   return rtt;
}

int CPeerManagement::getLastID(const string& ip, const int& port, const int& session)
{
   CPeerRecord pr;
   pr.m_strIP = ip;
   pr.m_iPort = port;
   pr.m_iSession = session;
   int id = -1;

   CGuard recguard(m_PeerRecLock);

   set<CPeerRecord*, CFPeerRec>::iterator i = m_sPeerRec.find(&pr);
   if (i != m_sPeerRec.end())
      id = (*i)->m_iID;

   return id;
}

void CPeerManagement::clearRTT(const string& ip)
{
   CPeerRecord pr;
   pr.m_strIP = ip;

   CGuard recguard(m_PeerRecLock);

   pair<multiset<CPeerRecord*, CFPeerRecByIP>::iterator, multiset<CPeerRecord*, CFPeerRecByIP>::iterator> p;
   p = m_sPeerRecByIP.equal_range(&pr);
   for (multiset<CPeerRecord*, CFPeerRecByIP>::iterator i = p.first; i != p.second; i ++)
      (*i)->m_iRTT = -1;
}

int CPeerManagement::flowControl(const std::string& ip, const int& port, const int& session)
{
   CPeerRecord pr;
   pr.m_strIP = ip;
   pr.m_iPort = port;
   pr.m_iSession = session;

   CGuard recguard(m_PeerRecLock);

   set<CPeerRecord*, CFPeerRec>::iterator i = m_sPeerRec.find(&pr);
   if (i == m_sPeerRec.end())
      return 0;


   int thresh = (*i)->m_iFlowWindow - (CTimer::getTime() - (*i)->m_llTimeStamp) / 1000;

   if (thresh > 100)
   {
      usleep(100000);
      return 100000;
   }

   if (thresh > 10)
   {
      usleep(10000);
      return 10000;
   }

   return 0;
}
