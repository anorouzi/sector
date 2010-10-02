/*****************************************************************************
Copyright (c) 2001 - 2010, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu, last updated 08/31/2010
*****************************************************************************/

#include "udt.h"
#include "common.h"
#include "epoll.h"
#include <errno.h>
#include <algorithm>
#include <iterator>
#if defined (LINUX) && !defined(__CYGWIN__)
   #include <sys/epoll.h>
   #include <unistd.h>
#endif

using namespace std;

CEPoll::CEPoll():
m_iIDSeed(0)
{
}

CEPoll::~CEPoll()
{
}

int CEPoll::create()
{
   CGuard pg(m_EPollLock);

   int localid = 0;

   #if defined (LINUX) && !defined(__CYGWIN__)
   localid = epoll_create(1024);
   if (localid < 0)
      throw CUDTException(-1, 0, errno);
   #else
   // on BSD, use kqueue
   // on Solaris, use /dev/poll
   // on Windows, select
   #endif

   if (++ m_iIDSeed >= 0x7FFFFFFF)
      m_iIDSeed = 0;

   CEPollDesc desc;
   desc.m_iID = m_iIDSeed;
   desc.m_iLocalID = localid;
   m_mPolls[desc.m_iID] = desc;

   return desc.m_iID;
}

int CEPoll::add(const int eid, const set<UDTSOCKET>* socks, const set<SYSSOCKET>* locals)
{
   CGuard pg(m_EPollLock);

   map<int, CEPollDesc>::iterator p = m_mPolls.find(eid);
   if (p == m_mPolls.end())
      throw CUDTException(5, 13);

   if (NULL != socks)
   {
      set<UDTSOCKET> res;
      set_union(p->second.m_sUDTSocks.begin(), p->second.m_sUDTSocks.end(), socks->begin(), socks->end(), inserter(res, res.begin()));
      p->second.m_sUDTSocks = res;
   }

   if (NULL != locals)
   {
      #if defined (LINUX) && !defined(__CYGWIN__)
      for (set<SYSSOCKET>::const_iterator i = locals->begin(); i != locals->end(); ++ i)
      {
         epoll_event ev;
         ev.events = EPOLLIN | EPOLLOUT | EPOLLERR;
         ev.data.fd = *i;
         if (epoll_ctl(p->second.m_iLocalID, EPOLL_CTL_ADD, *i, &ev) >= 0)
            p->second.m_sLocals.insert(*i);
      }
      #else
      for (set<SYSSOCKET>::const_iterator i = locals->begin(); i != locals->end(); ++ i)
         p->second.m_sLocals.insert(*i);
      #endif
   }

   return 0;
}

int CEPoll::remove(const int eid, const set<UDTSOCKET>* socks, const set<SYSSOCKET>* locals)
{
   CGuard pg(m_EPollLock);

   map<int, CEPollDesc>::iterator p = m_mPolls.find(eid);
   if (p == m_mPolls.end())
      throw CUDTException(5, 13);

   if (NULL != socks)
   {
      set<UDTSOCKET> res;
      set_difference(p->second.m_sUDTSocks.begin(), p->second.m_sUDTSocks.end(), socks->begin(), socks->end(), inserter(res, res.begin()));
      p->second.m_sUDTSocks = res;
   }

   if (NULL != locals)
   {
      #if defined (LINUX) && !defined(__CYGWIN__)
      for (set<SYSSOCKET>::const_iterator i = locals->begin(); i != locals->end(); ++ i)
      {
         epoll_event ev;
         ev.events = EPOLLIN | EPOLLOUT | EPOLLERR;
         ev.data.fd = *i;
         if (epoll_ctl(p->second.m_iLocalID, EPOLL_CTL_DEL, *i, &ev) >= 0)
            p->second.m_sLocals.erase(*i);
      }
      #else
      for (set<SYSSOCKET>::const_iterator i = locals->begin(); i != locals->end(); ++ i)
         p->second.m_sLocals.erase(*i);
      #endif
   }

   return 0;
}

int CEPoll::wait(const int eid, set<UDTSOCKET>* readfds, set<UDTSOCKET>* writefds, int64_t msTimeOut, set<SYSSOCKET>* lrfds, set<SYSSOCKET>* lwfds)
{
   int total = 0;

   int64_t entertime = CTimer::getTime();
   while (true)
   {
      m_EPollLock.acquire();

      map<int, CEPollDesc>::iterator p = m_mPolls.find(eid);
      if (p == m_mPolls.end())
      {
         m_EPollLock.release();
         throw CUDTException(5, 13);
      }

      if ((NULL != readfds) && !p->second.m_sUDTReads.empty())
      {
         *readfds = p->second.m_sUDTReads;
         total += p->second.m_sUDTReads.size();
      }

      if ((NULL != writefds) && !p->second.m_sUDTWrites.empty())
      {
         *writefds = p->second.m_sUDTWrites;
         total += p->second.m_sUDTWrites.size();
      }

      if (!lrfds || !lwfds)
      {
         if (lrfds)
            lrfds->clear();

         if (lwfds)
            lwfds->clear();

         #if defined (LINUX) && !defined(__CYGWIN__)
         const int max_events = p->second.m_sLocals.size();
         epoll_event ev[max_events];
         int nfds = epoll_wait(p->second.m_iLocalID, ev, max_events, 0);
         for (int i = 0; i < nfds; ++ i)
         {
            if ((NULL != lrfds) && (ev[i].events & EPOLLIN))
            {
               lrfds->insert(ev[i].data.fd);
               ++ total;
            }
            if ((NULL != lwfds) && (ev[i].events & EPOLLOUT))
            {
               lwfds->insert(ev[i].data.fd);
               ++ total;
            }
         }
         #else
         //currently "select" is used for all non-Linux platforms.
         //faster approaches can be applied for specific systems in the future.

         //"select" has a limitation on the number of sockets

         fd_set readfds;
         fd_set writefds;
         FD_ZERO(&readfds);
         FD_ZERO(&writefds);

         for (set<SYSSOCKET>::const_iterator i = p->second.m_sLocals.begin(); i != p->second.m_sLocals.end(); ++ i)
         {
            if (lrfds)
               FD_SET(*i, &readfds);
            if (lwfds)
               FD_SET(*i, &writefds);
         }

         timeval tv;
         tv.tv_sec = 0;
         tv.tv_usec = 0;
         int r = select(0, &readfds, &writefds, NULL, &tv);

         if (r > 0)
         {
            for (set<SYSSOCKET>::const_iterator i = p->second.m_sLocals.begin(); i != p->second.m_sLocals.end(); ++ i)
            {
               if (lrfds)
               {
                  if (FD_ISSET(*i, &readfds))
                  {
                     lrfds->insert(*i);
                     ++ total;
                  }
               }

               if (lwfds)
               {
                  if (FD_ISSET(*i, &writefds))
                  {
                     lwfds->insert(*i);
                     ++ total;
                  }
               }
            }
         }

         #endif
      }

      m_EPollLock.release();

      if (total > 0)
         return total;

      CTimer::waitForEvent();

      if ((msTimeOut >= 0) && (int64_t(CTimer::getTime() - entertime) >= msTimeOut))
         break;
   }

   return 0;
}

int CEPoll::release(const int eid)
{
   CGuard pg(m_EPollLock);

   map<int, CEPollDesc>::iterator i = m_mPolls.find(eid);
   if (i == m_mPolls.end())
      throw CUDTException(5, 13);

   #if defined (LINUX) && !defined(__CYGWIN__)
   // release local/system epoll descriptor
   ::close(i->second.m_iLocalID);
   #endif

   m_mPolls.erase(i);

   return 0;
}

int CEPoll::enable_write(const UDTSOCKET& uid, set<int>& eids)
{
   CGuard pg(m_EPollLock);

   map<int, CEPollDesc>::iterator p;

   vector<int> lost;
   for (set<int>::iterator i = eids.begin(); i != eids.end(); ++ i)
   {
      p = m_mPolls.find(*i);
      if (p == m_mPolls.end())
      {
         lost.push_back(*i);
      }
      else
      {
         p->second.m_sUDTWrites.insert(uid);
      }
   }

   for (vector<int>::iterator i = lost.begin(); i != lost.end(); ++ i)
      eids.erase(*i);

   return 0;
}

int CEPoll::enable_read(const UDTSOCKET& uid, set<int>& eids)
{
   CGuard pg(m_EPollLock);

   map<int, CEPollDesc>::iterator p;

   vector<int> lost;
   for (set<int>::iterator i = eids.begin(); i != eids.end(); ++ i)
   {
      p = m_mPolls.find(*i);
      if (p == m_mPolls.end())
      {
         lost.push_back(*i);
      }
      else
      {
         p->second.m_sUDTReads.insert(uid);
      }
   }

   for (vector<int>::iterator i = lost.begin(); i != lost.end(); ++ i)
      eids.erase(*i);

   return 0;
}

int CEPoll::disable_write(const UDTSOCKET& uid, set<int>& eids)
{
   CGuard pg(m_EPollLock);

   map<int, CEPollDesc>::iterator p;

   vector<int> lost;
   for (set<int>::iterator i = eids.begin(); i != eids.end(); ++ i)
   {
      p = m_mPolls.find(*i);
      if (p == m_mPolls.end())
      {
         lost.push_back(*i);
      }
      else
      {
         p->second.m_sUDTWrites.erase(uid);
      }
   }

   for (vector<int>::iterator i = lost.begin(); i != lost.end(); ++ i)
      eids.erase(*i);

   return 0;
}

int CEPoll::disable_read(const UDTSOCKET& uid, set<int>& eids)
{
   CGuard pg(m_EPollLock);

   map<int, CEPollDesc>::iterator p;

   vector<int> lost;
   for (set<int>::iterator i = eids.begin(); i != eids.end(); ++ i)
   {
      p = m_mPolls.find(*i);
      if (p == m_mPolls.end())
      {
         lost.push_back(*i);
      }
      else
      {
         p->second.m_sUDTReads.erase(uid);
      }
   }

   for (vector<int>::iterator i = lost.begin(); i != lost.end(); ++ i)
      eids.erase(*i);

   return 0;
}