/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 09/09/2010
*****************************************************************************/

#ifndef WIN32
   #include <sys/socket.h>
   #include <netinet/in.h>
   #include <arpa/inet.h>
   #include <pthread.h>
#endif
#include <cstring>
#include "common.h"
#include "datachn.h"

using namespace std;

ChnInfo::ChnInfo():
m_pTrans(NULL),
m_iCount(0),
m_llTotalQueueSize(0),
m_bSecKeySet(false)
{
}

ChnInfo::~ChnInfo()
{
   if (NULL != m_pTrans)
   {
      m_pTrans->close();
      delete m_pTrans;
      m_pTrans = NULL;
   }

   for (vector<RcvData>::iterator i = m_vDataQueue.begin(); i != m_vDataQueue.end(); ++ i)
      delete [] i->m_pcData;
}

DataChn::DataChn()
{
}

DataChn::~DataChn()
{
   // remove all allocated data structures, dangling connections, unread buffers, etc.
   for (map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.begin(); i != m_mChannel.end(); ++ i)
   {
      delete i->second;
   }

   m_mChannel.clear();

}

int DataChn::init(const string& ip, int& port)
{
   if (m_Base.open(port, true, true) < 0)
      return -1;

   m_strIP = ip;
   m_iPort = port;

   // add itself
   ChnInfo* c = new ChnInfo;
   c->m_iCount = 1;

   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;
   m_mChannel[addr] = c;

   return port;
}

int DataChn::garbageCollect()
{
   CGuard dg(m_ChnLock);

   vector<Address> tbd;

   // remove broken connections, which mean the peers have already left
   for (map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.begin(); i != m_mChannel.end(); ++ i)
   {
      if ((NULL == i->second->m_pTrans) || i->second->m_pTrans->isConnected())
         continue;

      delete i->second;

      tbd.push_back(i->first);
   }

   for (vector<Address>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
      m_mChannel.erase(*i);

   return 0;
}

bool DataChn::isConnected(const string& ip, int port)
{
   // no need to connect to self
   if ((ip == m_strIP) && (port == m_iPort))
      return true;

   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return false;

   // in case that another thread is calling collect(ip, port)
   // wait here until it is completed
   CGuard dg(c->m_SndLock);

   return ((NULL != c->m_pTrans) && c->m_pTrans->isConnected());
}

int DataChn::connect(const string& ip, int port)
{
   // no need to connect to self
   if ((ip == m_strIP) && (port == m_iPort))
      return 1;

   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;

   ChnInfo* c = locate(ip, port);
   if (NULL == c)
   {
      c = new ChnInfo;

      m_ChnLock.acquire();
      if (m_mChannel.find(addr) == m_mChannel.end())
         m_mChannel[addr] = c;
      else
      {
         delete c;
         c = m_mChannel[addr];
      }
      m_ChnLock.release();
   }

   // snd lock is used to prevent two threads from connecting on the same channel at the same time
   c->m_SndLock.acquire();

   if (NULL != c->m_pTrans)
   {
      if (c->m_pTrans->isConnected())
      {
         // data channel already exists, increase reference count, and return
         c->m_iCount ++;
         c->m_SndLock.release();
         return 0;
      }
      else
      {
         // the existing data channel is already broken, create a new one
         delete c->m_pTrans;
         c->m_pTrans = NULL;
      }
   }

   UDTTransport* t = new UDTTransport;
   t->open(m_iPort, true, true);
   int r = t->connect(ip.c_str(), port);

   if (r >= 0)
   {
      // new channel, first connection
      c->m_pTrans = t;
      c->m_iCount = 1;
   }
   else
   {
      delete t;
   }

   c->m_SndLock.release();

   return r;
}

int DataChn::remove(const string& ip, int port)
{
   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;

   m_ChnLock.acquire();
   map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.find(addr);
   if (i != m_mChannel.end())
   {
      -- i->second->m_iCount;
      if (0 == i->second->m_iCount)
      {
         // disconnect
         if ((NULL != i->second->m_pTrans) && i->second->m_pTrans->isConnected())
            i->second->m_pTrans->close();

         // wait for all send/recv to complete
         i->second->m_SndLock.acquire();
         i->second->m_SndLock.release();
         i->second->m_RcvLock.acquire();
         i->second->m_RcvLock.release();

         delete i->second;

         m_mChannel.erase(i);
      }
   }
   m_ChnLock.release();

   return 0;
}

int DataChn::setCryptoKey(const string& ip, int port, unsigned char key[16], unsigned char iv[8])
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   // crypto key should only be set once for each connection
   // otherwise, encryption can be wrong due to key reset

   CGuard cg(c->m_SndLock);

   if (c->m_bSecKeySet)
      return 0;

   c->m_pTrans->initCoder(key, iv);
   c->m_bSecKeySet = true;

   return 0;
}

ChnInfo* DataChn::locate(const string& ip, int port)
{
   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;

   m_ChnLock.acquire();
   map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.find(addr);
   if ((i == m_mChannel.end()) || ((NULL != i->second->m_pTrans) && !i->second->m_pTrans->isConnected()))
   {
       m_ChnLock.release();
      return NULL;
   }
   m_ChnLock.release();

   return i->second;
}

int DataChn::send(const string& ip, int port, int session, const char* data, int size, bool secure)
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   if ((ip == m_strIP) && (port == m_iPort))
   {
      // send data to self
      RcvData q;
      q.m_iSession = session;
      q.m_pcData = NULL;
      q.m_iSize = size;
      if (size > 0)
      {
         q.m_pcData = new char[size];
         memcpy(q.m_pcData, data, size);
      }

      c->m_QueueLock.acquire();
      c->m_llTotalQueueSize += q.m_iSize;
      c->m_vDataQueue.push_back(q);
      c->m_QueueLock.release();

      return size;
   }

   c->m_SndLock.acquire();

   if (NULL == c->m_pTrans)
   {
      // no connection
      c->m_SndLock.release();
      return -1;
   }

   c->m_pTrans->send((char*)&session, 4);
   c->m_pTrans->send((char*)&size, 4);
   if (size > 0)
      c->m_pTrans->sendEx(data, size, secure);

   c->m_SndLock.release();

   return size;
}

int DataChn::recv(const string& ip, int port, int session, char*& data, int& size, bool secure)
{
   data = NULL;

   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   bool self = (ip == m_strIP) && (port == m_iPort);
   if (!self && ((NULL == c->m_pTrans) || !c->m_pTrans->isConnected()))
      return -1;

   while (true)
   {
       c->m_QueueLock.acquire();
      for (vector<RcvData>::iterator q = c->m_vDataQueue.begin(); q != c->m_vDataQueue.end(); ++ q)
      {
         if (session == q->m_iSession)
         {
            size = q->m_iSize;
            data = q->m_pcData;
            c->m_llTotalQueueSize -= q->m_iSize;
            c->m_vDataQueue.erase(q);

            c->m_QueueLock.release();
            return size;
         }
      }
      c->m_QueueLock.release();

      if (c->m_RcvLock.trylock() == false)
      {
         // if another thread is receiving data, wait a little while and check the queue again
         CTimer::sleep();
         continue;
      }

      bool found = false;
      c->m_QueueLock.acquire();
      for (vector<RcvData>::iterator q = c->m_vDataQueue.begin(); q != c->m_vDataQueue.end(); ++ q)
      {
         if (session == q->m_iSession)
         {
            size = q->m_iSize;
            data = q->m_pcData;
            c->m_llTotalQueueSize -= q->m_iSize;
            c->m_vDataQueue.erase(q);

            found = true;
            break;
         }
      }
      c->m_QueueLock.release();

      if (found)
      {
          c->m_RcvLock.release();
         return size;
      }

      if (self)
      {
         // if this is local recv, just wait for the sender (aka itself) to pass the data
          c->m_RcvLock.release();
         CTimer::sleep();
         continue;
      }
      else if ((NULL == c->m_pTrans) && !c->m_pTrans->isConnected())
      {
         // no connection
         c->m_RcvLock.release();
         return -1;
      }

      RcvData rd;
      rd.m_pcData = NULL;
      rd.m_iSize = 0;
      if (c->m_pTrans->recv((char*)&rd.m_iSession, 4) < 0)
      {
          c->m_RcvLock.release();
          return -1;
      }
      if (c->m_pTrans->recv((char*)&rd.m_iSize, 4) < 0)
      {
          c->m_RcvLock.release();
          return -1;
      }

      if (rd.m_iSize < 0)
      {
         // the peer may send a negative size to indicate error, see sendError
         rd.m_pcData = NULL;
      }
      else
      {
         try
         {
            rd.m_pcData = new char[rd.m_iSize];
         }
         catch (...)
         {
            c->m_RcvLock.release();
            return -1;
         }

         if (c->m_pTrans->recvEx(rd.m_pcData, rd.m_iSize, secure) < 0)
         {
            delete [] rd.m_pcData;
            c->m_RcvLock.release();
            return -1;
         }
      }

      if (session == rd.m_iSession)
      {
         size = rd.m_iSize;
         data = rd.m_pcData;
         c->m_RcvLock.release();
         return size;
      }

      c->m_QueueLock.acquire();
      if (rd.m_iSize > 0)
         c->m_llTotalQueueSize += rd.m_iSize;
      c->m_vDataQueue.push_back(rd);
      c->m_QueueLock.release();

      c->m_RcvLock.release();
   }

   size = 0;
   data = NULL;
   return -1;
}

int64_t DataChn::sendfile(const string& ip, int port, int session, fstream& ifs, int64_t offset, int64_t size, bool secure)
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   if ((ip == m_strIP) && (port == m_iPort))
   {
      // send data to self
      RcvData q;
      q.m_iSession = session;
      q.m_pcData = NULL;
      q.m_iSize = static_cast<int>(size);
      if (size > 0)
      {
         q.m_pcData = new char[static_cast<size_t>(size)];
         ifs.seekg(offset);
         ifs.read(q.m_pcData, size);
      }

      c->m_QueueLock.acquire();
      c->m_llTotalQueueSize += q.m_iSize;
      c->m_vDataQueue.push_back(q);
      c->m_QueueLock.release();

      return size;
   }

   c->m_SndLock.acquire();

   if (NULL == c->m_pTrans)
   {
      // no connection
      c->m_SndLock.release();
      return -1;
   }

   c->m_pTrans->send((char*)&session, 4);
   c->m_pTrans->send((char*)&size, 4);
   if (size > 0)
      c->m_pTrans->sendfileEx(ifs, offset, size, secure);

   c->m_SndLock.release();

   return size;
}

int64_t DataChn::recvfile(const string& ip, int port, int session, fstream& ofs, int64_t offset, int64_t& size, bool secure)
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   bool self = (ip == m_strIP) && (port == m_iPort);
   if (!self && ((NULL == c->m_pTrans) || !c->m_pTrans->isConnected()))
      return -1;

   while (true)
   {
       c->m_QueueLock.acquire();
      for (vector<RcvData>::iterator q = c->m_vDataQueue.begin(); q != c->m_vDataQueue.end(); ++ q)
      {
         if (session == q->m_iSession)
         {
            size = q->m_iSize;
            ofs.seekp(offset);
            ofs.write(q->m_pcData, size);
            delete [] q->m_pcData;
            c->m_vDataQueue.erase(q);

            c->m_QueueLock.release();
            return size;
         }
      }
      c->m_QueueLock.release();

      if (c->m_RcvLock.trylock() == false)
      {
         // if another thread is receiving data, wait a little while and check the queue again
         CTimer::sleep();
         continue;
      }

      bool found = false;
      c->m_QueueLock.acquire();
      for (vector<RcvData>::iterator q = c->m_vDataQueue.begin(); q != c->m_vDataQueue.end(); ++ q)
      {
         if (session == q->m_iSession)
         {
            size = q->m_iSize;
            ofs.seekp(offset);
            ofs.write(q->m_pcData, size);
            delete [] q->m_pcData;
            c->m_vDataQueue.erase(q);

            found = true;
            break;
         }
      }
      c->m_QueueLock.release();

      if (found)
      {
          c->m_RcvLock.release();
         return size;
      }

      if (self)
      {
         // if this is local recv, just wait for the sender (aka itself) to pass the data
          c->m_RcvLock.release();
         CTimer::sleep();
         continue;
      }
      else if ((NULL == c->m_pTrans) && !c->m_pTrans->isConnected())
      {
         // no connection
         c->m_RcvLock.release();
         return -1;
      }

      RcvData rd;
      rd.m_pcData = NULL;
      rd.m_iSize = 0;
      if (c->m_pTrans->recv((char*)&rd.m_iSession, 4) < 0)
      {
          c->m_RcvLock.release();
         return -1;
      }
      if (c->m_pTrans->recv((char*)&rd.m_iSize, 4) < 0)
      {
          c->m_RcvLock.release();
         return -1;
      }

      if (rd.m_iSize < 0)
      {
         rd.m_pcData = NULL;
      }
      else
      {
         if (session == rd.m_iSession)
         {
            //TODO: if recvfile returns error, following recv will crash
            //recvfile should be removed in future version, and maybe messaging mode can be considered

            if (c->m_pTrans->recvfileEx(ofs, offset, rd.m_iSize, secure) < 0)
            {
                c->m_RcvLock.release();
               return -1;
            }
         }
         else
         {
            try
            {
               rd.m_pcData = new char[rd.m_iSize];
            }
            catch (...)
            {
                c->m_RcvLock.release();
               return -1;
            }

            if (c->m_pTrans->recvEx(rd.m_pcData, rd.m_iSize, secure) < 0)
            {
               delete [] rd.m_pcData;
               c->m_RcvLock.release();
               return -1;
            }
         }
      }

      if (session == rd.m_iSession)
      {
         size = rd.m_iSize;
         c->m_RcvLock.release();
         return size;
      }

      c->m_QueueLock.acquire();
      if (rd.m_iSize > 0)
         c->m_llTotalQueueSize += rd.m_iSize;
      c->m_vDataQueue.push_back(rd);
      c->m_QueueLock.release();

      c->m_RcvLock.release();
   }

   size = 0;
   return -1;
}

int DataChn::recv4(const string& ip, int port, int session, int32_t& val)
{
   char* buf = NULL;
   int size = 4;
   if (recv(ip, port, session, buf, size) < 0)
      return -1;

   if (size != 4)
   {
      delete [] buf;
      return -1;
   }

   val = *(int32_t*)buf;
   delete [] buf;
   return 4;
}

int DataChn::recv8(const string& ip, int port, int session, int64_t& val)
{
   char* buf = NULL;
   int size = 8;
   if (recv(ip, port, session, buf, size) < 0)
      return -1;

   if (size != 8)
   {
      delete [] buf;
      return -1;
   }

   val = *(int64_t*)buf;
   delete [] buf;
   return 8;
}

int64_t DataChn::getRealSndSpeed(const string& ip, int port)
{
   ChnInfo* c = locate(ip, port);
   if ((NULL == c) || (NULL == c->m_pTrans))
      return -1;

   return c->m_pTrans->getRealSndSpeed();
}

int DataChn::getSelfAddr(const string& peerip, int peerport, string& localip, int& localport)
{
   ChnInfo* c = locate(peerip, peerport);
   if (NULL == c)
      return -1;

   if ((peerip == m_strIP) && (peerport == m_iPort))
   {
      localip = m_strIP;
      localport = m_iPort;
      return 0;
   }

   sockaddr_in addr;
   if (c->m_pTrans->getsockname((sockaddr*)&addr) < 0)
      return -1;

   localip = inet_ntoa(addr.sin_addr);
   localport = ntohs(addr.sin_port);
   return 0;
}

int DataChn::sendError(const string& ip, int port, int session)
{
   return send(ip, port, session, NULL, -1);
}
