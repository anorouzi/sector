/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 03/14/2009
*****************************************************************************/

#include <pthread.h>
#include <datachn.h>

using namespace std;

DataChn::DataChn()
{
   pthread_mutex_init(&m_ChnLock, NULL);
}

DataChn::~DataChn()
{
   for (map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.begin(); i != m_mChannel.end(); ++ i)
   {
      if (NULL != i->second->m_pTrans)
      {
         i->second->m_pTrans->close();
         delete i->second->m_pTrans;
      }
      pthread_mutex_destroy(&i->second->m_SndLock);
      pthread_mutex_destroy(&i->second->m_RcvLock);
      pthread_mutex_destroy(&i->second->m_QueueLock);

      for (vector<RcvData>::iterator j = i->second->m_vDataQueue.begin(); j != i->second->m_vDataQueue.end(); ++ j)
         delete [] j->m_pcData;

      delete i->second;
   }

   m_mChannel.clear();

   pthread_mutex_destroy(&m_ChnLock);
}

int DataChn::init(const string& ip, int& port)
{
   if (m_Base.open(port, true, true) < 0)
      return -1;

   m_strIP = ip;
   m_iPort = port;

   // add itself
   ChnInfo* c = new ChnInfo;
   c->m_pTrans = NULL;
   pthread_mutex_init(&c->m_SndLock, NULL);
   pthread_mutex_init(&c->m_QueueLock, NULL);
   pthread_mutexattr_t attr;
   pthread_mutexattr_init(&attr);
   pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
   pthread_mutex_init(&c->m_RcvLock, &attr);
   pthread_mutexattr_destroy(&attr);

   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;
   m_mChannel[addr] = c;

   return port;
}

bool DataChn::isConnected(const std::string& ip, int port)
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return false;

   return c->m_pTrans->isConnected();
}

int DataChn::connect(const string& ip, int port)
{
   // no need to connect to self
   if ((ip == m_strIP) && (port == m_iPort))
      return 1;

   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;

   pthread_mutex_lock(&m_ChnLock);
   map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.find(addr);
   if (i != m_mChannel.end())
   {
      if (i->second->m_pTrans->isConnected())
      {
         pthread_mutex_unlock(&m_ChnLock);
         return 0;
      }
      delete i->second->m_pTrans;
   }
   pthread_mutex_unlock(&m_ChnLock);

   Transport* t = new Transport;
   t->open(m_iPort, true, true);
   t->connect(ip.c_str(), port);

   pthread_mutex_lock(&m_ChnLock);
   if (i != m_mChannel.end())
   {
      i->second->m_pTrans = t;
   }
   else
   {
      ChnInfo* c = new ChnInfo;
      c->m_pTrans = t;
      pthread_mutex_init(&c->m_SndLock, NULL);
      pthread_mutex_init(&c->m_QueueLock, NULL);
      pthread_mutexattr_t attr;
      pthread_mutexattr_init(&attr);
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
      pthread_mutex_init(&c->m_RcvLock, &attr);
      pthread_mutexattr_destroy(&attr);
      m_mChannel[addr] = c;
   }
   pthread_mutex_unlock(&m_ChnLock);

   return 1;
}

int DataChn::remove(const std::string& ip, int port)
{
   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;

   pthread_mutex_lock(&m_ChnLock);
   map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.find(addr);
   if (i != m_mChannel.end())
   {
      if ((NULL != i->second->m_pTrans) && i->second->m_pTrans->isConnected())
         i->second->m_pTrans->close();
      delete i->second->m_pTrans;
      pthread_mutex_destroy(&i->second->m_SndLock);
      pthread_mutex_destroy(&i->second->m_RcvLock);
      pthread_mutex_destroy(&i->second->m_QueueLock);
      m_mChannel.erase(i);
   }
   pthread_mutex_unlock(&m_ChnLock);

   return 0;
}

int DataChn::setCryptoKey(const std::string& ip, int port, unsigned char key[16], unsigned char iv[8])
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   c->m_pTrans->initCoder(key, iv);

   return 0;
}

DataChn::ChnInfo* DataChn::locate(const std::string& ip, int port)
{
   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;

   pthread_mutex_lock(&m_ChnLock);
   map<Address, ChnInfo*, AddrComp>::iterator i = m_mChannel.find(addr);
   if ((i == m_mChannel.end()) || ((NULL != i->second->m_pTrans) && !i->second->m_pTrans->isConnected()))
   {
       pthread_mutex_unlock(&m_ChnLock);
       return NULL;
   }
   pthread_mutex_unlock(&m_ChnLock);

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
      q.m_iSize = size;
      q.m_pcData = new char[size];
      memcpy(q.m_pcData, data, size);

      pthread_mutex_lock(&c->m_QueueLock);
      c->m_vDataQueue.push_back(q);
      pthread_mutex_unlock(&c->m_QueueLock);

      return size;
   }

   pthread_mutex_lock(&c->m_SndLock);
   c->m_pTrans->send((char*)&session, 4);
   c->m_pTrans->send((char*)&size, 4);
   if (!secure)
      c->m_pTrans->send(data, size);
   else
      c->m_pTrans->sendEx(data, size, true);
   pthread_mutex_unlock(&c->m_SndLock);

   return size;
}

int DataChn::recv(const string& ip, int port, int session, char*& data, int& size, bool secure)
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   while ((NULL == c->m_pTrans) || c->m_pTrans->isConnected())
   {
      if (pthread_mutex_lock(&c->m_RcvLock) != 0)
      {
         // if another thread is receiving data, wait a little while and check the queue again
         usleep(10);
         continue;
      }

      bool found = false;
      pthread_mutex_lock(&c->m_QueueLock);
      for (vector<RcvData>::iterator q = c->m_vDataQueue.begin(); q != c->m_vDataQueue.end(); ++ q)
      {
         if (session == q->m_iSession)
         {
            size = q->m_iSize;
            data = q->m_pcData;
            c->m_vDataQueue.erase(q);

            found = true;
            break;
         }
      }
      pthread_mutex_unlock(&c->m_QueueLock);

      if (found)
      {
         pthread_mutex_unlock(&c->m_RcvLock);
         return size;
      }

      if (NULL == c->m_pTrans)
      {
         // if this is local recv, just wait for the sender (aka itself) to pass the data
         pthread_mutex_unlock(&c->m_RcvLock);
         usleep(10);
         continue;
      }

      RcvData rd;
      if (c->m_pTrans->recv((char*)&rd.m_iSession, 4) < 0)
      {
         pthread_mutex_unlock(&c->m_RcvLock);
         return -1;
      }
      if (c->m_pTrans->recv((char*)&rd.m_iSize, 4) < 0)
      {
         pthread_mutex_unlock(&c->m_RcvLock);
         return -1;
      }
      if (!secure)
      {
         rd.m_pcData = new char[rd.m_iSize];
         if (c->m_pTrans->recv(rd.m_pcData, rd.m_iSize) < 0)
         {
            delete [] rd.m_pcData;
            pthread_mutex_unlock(&c->m_RcvLock);
            return -1;
         }
      }
      else
      {
         rd.m_pcData = new char[rd.m_iSize + 64];
         if (c->m_pTrans->recvEx(rd.m_pcData, rd.m_iSize, true) < 0)
         {
            delete [] rd.m_pcData;
            pthread_mutex_unlock(&c->m_RcvLock);
            return -1;
         }
      }
      pthread_mutex_unlock(&c->m_RcvLock);


      if (session == rd.m_iSession)
      {
         size = rd.m_iSize;
         data = rd.m_pcData;
         return size;
      }

      pthread_mutex_lock(&c->m_QueueLock);
      c->m_vDataQueue.push_back(rd);
      pthread_mutex_unlock(&c->m_QueueLock);
   }

   size = 0;
   data = NULL;
   return -1;
}

int64_t DataChn::sendfile(const string& ip, int port, int session, ifstream& ifs, int64_t offset, int64_t size, bool secure)
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   if ((ip == m_strIP) && (port == m_iPort))
   {
      // send data to self
      RcvData q;
      q.m_iSession = session;
      q.m_iSize = size;
      q.m_pcData = new char[size];
      ifs.seekg(offset);
      ifs.read(q.m_pcData, size);

      pthread_mutex_lock(&c->m_QueueLock);
      c->m_vDataQueue.push_back(q);
      pthread_mutex_unlock(&c->m_QueueLock);

      return size;
   }

   pthread_mutex_lock(&c->m_SndLock);
   c->m_pTrans->send((char*)&session, 4);
   c->m_pTrans->send((char*)&size, 4);
   c->m_pTrans->sendfile(ifs, offset, size);
   pthread_mutex_unlock(&c->m_SndLock);

   return size;
}

int64_t DataChn::recvfile(const string& ip, int port, int session, ofstream& ofs, int64_t offset, int64_t& size, bool secure)
{
   ChnInfo* c = locate(ip, port);
   if (NULL == c)
      return -1;

   while ((NULL == c->m_pTrans) || c->m_pTrans->isConnected())
   {
      if (pthread_mutex_lock(&c->m_RcvLock) != 0)
      {
         // if another thread is receiving data, wait a little while and check the queue again
         usleep(10);
         continue;
      }

      bool found = false;
      pthread_mutex_lock(&c->m_QueueLock);
      for (vector<RcvData>::iterator q = c->m_vDataQueue.begin(); q != c->m_vDataQueue.end(); ++ q)
      {
         if (session == q->m_iSession)
         {
            size = q->m_iSize;
            ofs.seekp(offset);
            ofs.write(q->m_pcData, size);
            c->m_vDataQueue.erase(q);

            found = true;
            break;
         }
      }
      pthread_mutex_unlock(&c->m_QueueLock);

      if (found)
      {
         pthread_mutex_unlock(&c->m_RcvLock);
         return size;
      }

      if (NULL == c->m_pTrans)
      {
         // if this is local recv, just wait for the sender (aka itself) to pass the data
         pthread_mutex_unlock(&c->m_RcvLock);
         usleep(10);
         continue;
      }

      RcvData rd;
      if (c->m_pTrans->recv((char*)&rd.m_iSession, 4) < 0)
      {
         pthread_mutex_unlock(&c->m_RcvLock);
         return -1;
      }
      if (c->m_pTrans->recv((char*)&rd.m_iSize, 4) < 0)
      {
         pthread_mutex_unlock(&c->m_RcvLock);
         return -1;
      }
      if (!secure)
      {
         if (session == rd.m_iSession)
         {
            if (c->m_pTrans->recvfile(ofs, offset, rd.m_iSize) < 0)
            {
               pthread_mutex_unlock(&c->m_RcvLock);
               return -1;
            }
         }
         else
         {
            rd.m_pcData = new char[rd.m_iSize];
            if (c->m_pTrans->recv(rd.m_pcData, rd.m_iSize) < 0)
            {
               delete [] rd.m_pcData;
               pthread_mutex_unlock(&c->m_RcvLock);
               return -1;
            }
         }
      }
      else
      {
         if (session == rd.m_iSession)
         {
            if (c->m_pTrans->recvfileEx(ofs, offset, rd.m_iSize, true) < 0)
            {
               pthread_mutex_unlock(&c->m_RcvLock);
               return -1;
            }
         }
         else
         {
            rd.m_pcData = new char[rd.m_iSize + 64];
            if (c->m_pTrans->recvEx(rd.m_pcData, rd.m_iSize, true) < 0)
            {
               delete [] rd.m_pcData;
               pthread_mutex_unlock(&c->m_RcvLock);
               return -1;
            }
         }
      }
      pthread_mutex_unlock(&c->m_RcvLock);


      if (session == rd.m_iSession)
      {
         size = rd.m_iSize;
         return size;
      }


      pthread_mutex_lock(&c->m_QueueLock);
      c->m_vDataQueue.push_back(rd);
      pthread_mutex_unlock(&c->m_QueueLock);
   }

   size = 0;
   return -1;
}

int DataChn::recv4(const std::string& ip, int port, int session, int32_t& val)
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

int DataChn::recv8(const std::string& ip, int port, int session, int64_t& val)
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
   return 4;
}

