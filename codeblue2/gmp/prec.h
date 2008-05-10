/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Group Messaging Protocol (GMP)

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

GMP is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

GMP is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 01/25/2007
*****************************************************************************/


#ifndef __PEER_RECORD_H__
#define __PEER_RECORD_H__

#ifndef WIN32
   #include <pthread.h>
#else
   #include <udt.h>
   #include <common.h>
#endif

#include <set>
#include <string>
using namespace std;

struct CPeerRecord
{
   string m_strIP;
   int m_iPort;
   int m_iSession;
   int32_t m_iID;
   int64_t m_llTimeStamp;
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
      return (p1->m_llTimeStamp > p2->m_llTimeStamp);
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
   void clearRTT(const string& ip);

private:
   set<CPeerRecord*, CFPeerRec> m_sPeerRec;
   set<CPeerRecord*, CFPeerRecByTS> m_sPeerRecByTS;
   multiset<CPeerRecord*, CFPeerRecByIP> m_sPeerRecByIP;

   pthread_mutex_t m_PeerRecLock;

private:
   static const unsigned int m_uiRecLimit = 500;   
};

#endif
