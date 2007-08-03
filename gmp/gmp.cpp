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
   Yunhong Gu [gu@lac.uic.edu], last updated 08/02/2007
*****************************************************************************/


#ifndef WIN32
   #include <unistd.h>
   #include <stdio.h>
   #include <errno.h>
   #include <assert.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif

#include <gmp.h>
#include <iostream>

using namespace std;
using namespace cb;

int32_t CGMPMessage::g_iSession = CGMPMessage::initSession();
int32_t CGMPMessage::g_iID = 1;
#ifndef WIN32
   pthread_mutex_t CGMPMessage::g_IDLock = PTHREAD_MUTEX_INITIALIZER;
#else
   pthread_mutex_t CGMPMessage::g_IDLock = CreateMutex(NULL, false, NULL);
#endif

CGMPMessage::CGMPMessage():
m_iType(m_piHeader[0]),
m_iSession(m_piHeader[1]),
m_iID(m_piHeader[2]),
m_iInfo(m_piHeader[3]),
m_pcData(NULL),
m_iLength(0)
{
   m_iSession = g_iSession;

   Sync::enterCS(g_IDLock);
   m_iID = g_iID ++;
   if (g_iID == g_iMaxID)
      g_iID = 1;
   Sync::leaveCS(g_IDLock);
}

CGMPMessage::CGMPMessage(const CGMPMessage& msg):
m_iType(m_piHeader[0]),
m_iSession(m_piHeader[1]),
m_iID(m_piHeader[2]),
m_iInfo(m_piHeader[3]),
m_pcData(NULL),
m_iLength(0)
{
   memcpy((char*)m_piHeader, (char*)msg.m_piHeader, 16);
   if (m_iLength > 0)
   {
      m_pcData = new char[m_iLength];
      memcpy(m_pcData, msg.m_pcData, m_iLength);
   }
}

CGMPMessage::~CGMPMessage()
{
   if (NULL != m_pcData)
      delete [] m_pcData;
}

void CGMPMessage::pack(const char* data, const int& len, const int32_t& info)
{
   m_iType = 0;

   if (len > 0)
   {
      if (NULL != m_pcData)
         delete [] m_pcData;

      m_iLength = len;
      m_pcData = new char[len];
      memcpy(m_pcData, data, len);
   }
   else
      m_iLength = 0;

   m_iInfo = info;
}

void CGMPMessage::pack(const int32_t& type, const int32_t& info)
{
   m_iType = type;
   m_iInfo = info;
}

int32_t CGMPMessage::initSession()
{
   srand(Time::getTime());
   return (int32_t)(rand() + 1);
}


CGMP::CGMP()
{
   Sync::initMutex(m_SndQueueLock);
   Sync::initCond(m_SndQueueCond);
   Sync::initMutex(m_RcvQueueLock);
   Sync::initCond(m_RcvQueueCond);
   Sync::initMutex(m_ResQueueLock);
   Sync::initCond(m_ResQueueCond);
   Sync::initMutex(m_RTTLock);
   Sync::initCond(m_RTTCond);

   m_bInit = false;
   m_bClosed = false;
}

CGMP::~CGMP()
{
   Sync::releaseMutex(m_SndQueueLock);
   Sync::releaseCond(m_SndQueueCond);
   Sync::releaseMutex(m_RcvQueueLock);
   Sync::releaseCond(m_RcvQueueCond);
   Sync::releaseMutex(m_ResQueueLock);
   Sync::releaseCond(m_ResQueueCond);
   Sync::releaseMutex(m_RTTLock);
   Sync::releaseCond(m_RTTCond);
}

int CGMP::init(const int& port)
{
   if (port != 0)
      m_iPort = port - 1;
   else
      m_iPort = 0;

   if ((m_UDTSocket.open(m_iPort, false) < 0) || (m_UDTSocket.listen() < 0))
      return -1;

   m_iPort ++;

   sockaddr_in addr;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(m_iPort);
   addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(addr.sin_zero), '\0', 8);

   m_UDPSocket = socket(AF_INET, SOCK_DGRAM, 0);
   if (0 != ::bind(m_UDPSocket, (sockaddr *)&addr, sizeof(sockaddr_in)))
   {
      perror("bind");
      return -1;
   }

   timeval tv;
   tv.tv_sec = 1;
   tv.tv_usec = 0;
   setsockopt(m_UDPSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(timeval));

   #ifndef WIN32
      pthread_create(&m_SndThread, NULL, sndHandler, this);
      pthread_create(&m_RcvThread, NULL, rcvHandler, this);
      pthread_create(&m_UDTRcvThread, NULL, udtRcvHandler, this);
   #else
      m_SndThread = CreateThread(NULL, 0, sndHandler, this, 0, NULL);
      m_RcvThread = CreateThread(NULL, 0, rcvHandler, this, 0, NULL);
      m_UDTRcvThread = CreateThread(NULL, 0, udtRcvHandler, this, 0, NULL);
   #endif

   m_bInit = true;

   return 1;
}

int CGMP::close()
{
   if (!m_bInit)
      return 1;

   m_bClosed = true;

   #ifndef WIN32
      pthread_mutex_lock(&m_SndQueueLock);
      pthread_cond_signal(&m_SndQueueCond);
      pthread_mutex_unlock(&m_SndQueueLock);

      pthread_join(m_SndThread, NULL);
      pthread_join(m_RcvThread, NULL);
      pthread_join(m_UDTRcvThread, NULL);
   #else
      SetEvent(m_SndQueueCond);
      WaitForSingleObject(m_SndThread, INFINITE);
      WaitForSingleObject(m_RcvThread, INFINITE);
      WaitForSingleObject(m_UDTRcvThread, INFINITE);
   #endif

   closesocket(m_UDPSocket);
   m_UDTSocket.close();

   #ifdef WIN32
      WSACleanup();
   #endif

   return 1;
}

int CGMP::getPort()
{
   return m_iPort;
}

int CGMP::sendto(const char* ip, const int& port, int32_t& id, const CUserMessage* msg)
{
   if (msg->m_iDataLength <= m_iMaxUDPMsgSize)
      return UDPsend(ip, port, id, msg->m_pcBuffer, msg->m_iDataLength, true);
   else
      return UDTsend(ip, port - 1, id, msg->m_pcBuffer, msg->m_iDataLength);
}

int CGMP::UDPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable)
{
   CGMPMessage* msg = new CGMPMessage;
   msg->pack(data, len, id);
   id = msg->m_iID;

   int res = UDPsend(ip, port, msg);
   if (res < 0)
      return -1;

   if (reliable)
   {
      CMsgRecord* rec = new CMsgRecord;
      strcpy(rec->m_pcIP, ip);
      rec->m_iPort = port;
      rec->m_pMsg = msg;
      rec->m_llTimeStamp = Time::getTime();

      Sync::enterCS(m_SndQueueLock);
      m_lSndQueue.push_back(rec);
      Sync::leaveCS(m_SndQueueLock);
   }
   else
   {
      delete msg;
   }

   return res;
}

int CGMP::UDPsend(const char* ip, const int& port, CGMPMessage* msg)
{
   sockaddr_in addr;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   #ifndef WIN32
      if (inet_pton(AF_INET, ip, &(addr.sin_addr)) <= 0)
         return -1;
   #else
      if (INADDR_NONE == (addr.sin_addr.s_addr = inet_addr(ip)))
         return -1;
   #endif
   memset(&(addr.sin_zero), '\0', 8);

   #ifndef WIN32
      iovec vec[2];
      vec[0].iov_base = msg->m_piHeader;
      vec[0].iov_len = 16;
      vec[1].iov_base = msg->m_pcData;
      vec[1].iov_len = msg->m_iLength;

      msghdr mh;
      mh.msg_name = &addr;
      mh.msg_namelen = sizeof(sockaddr_in);
      mh.msg_iov = vec;
      mh.msg_iovlen = 2;
      mh.msg_control = NULL;
      mh.msg_controllen = 0;
      mh.msg_flags = 0;

      sendmsg(m_UDPSocket, &mh, 0);
   #else
      WSABUF vec[2];
      vec[0].buf = (char*)msg->m_piHeader;
      vec[0].len = 16;
      vec[1].buf = msg->m_pcData;
      vec[1].len = msg->m_iLength;

      DWORD ssize;
      WSASendTo(m_UDPSocket, vec, 2, &ssize, 0, (sockaddr*)&addr, sizeof(sockaddr_in), NULL, NULL);
   #endif

   return 16 + msg->m_iLength;
}

int CGMP::UDTsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len)
{
   CGMPMessage* msg = new CGMPMessage;
   msg->pack(data, len, id);
   id = msg->m_iID;

   int res = UDTsend(ip, port, msg);

   delete msg;
   return res;
}

int CGMP::UDTsend(const char* ip, const int& port, CGMPMessage* msg)
{
   Transport t;
   int dataport = 0;

   if (t.open(dataport, false) < 0)
      return -1;

   if (t.connect(ip, port) < 0)
   {
      t.close();
      return -1;
   }

   if ((t.send((char*)(&m_iPort), 4) < 0) || (t.send((char*)(msg->m_piHeader), 16) < 0) || (t.send((char*)&(msg->m_iLength), 4) < 0))
   {
      t.close();
      return -1;
   }

   if (t.send(msg->m_pcData, msg->m_iLength) < 0)
   {
      t.close();
      return -1;
   }

   t.close();
   return 16 + msg->m_iLength;
}

int CGMP::recvfrom(char* ip, int& port, int32_t& id, CUserMessage* msg, const bool& block)
{
   bool timeout = false;

   Sync::enterCS(m_RcvQueueLock);

   while (!m_bClosed && m_qRcvQueue.empty() && !timeout)
   {
      #ifndef WIN32
         if (block)
            pthread_cond_wait(&m_RcvQueueCond, &m_RcvQueueLock);
         else
         {
            timeval now;
            timespec expiretime;
            gettimeofday(&now, 0);
            expiretime.tv_sec = now.tv_sec + 1;
            expiretime.tv_nsec = now.tv_usec * 1000;
            if (pthread_cond_timedwait(&m_RcvQueueCond, &m_RcvQueueLock, &expiretime) != 0)
               timeout = true;
         }
      #else
         ReleaseMutex(m_RcvQueueLock);
         if (block)
            WaitForSingleObject(m_RcvQueueCond, INFINITE);
         else
         {
            if (WaitForSingleObject(m_RcvQueueCond, 1000) == WAIT_TIMEOUT)
               timeout = true;
         }
         WaitForSingleObject(m_RcvQueueLock, INFINITE);
      #endif
   }

   if (m_bClosed || timeout)
   {
      Sync::leaveCS(m_RcvQueueLock);
      return -1;
   }

   CMsgRecord* rec = m_qRcvQueue.front();
   m_qRcvQueue.pop();

   Sync::leaveCS(m_RcvQueueLock);

   strcpy(ip, rec->m_pcIP);
   port = rec->m_iPort;
   id = rec->m_pMsg->m_iID;

   if (msg->m_iBufLength < rec->m_pMsg->m_iLength)
      msg->resize(rec->m_pMsg->m_iLength);
   msg->m_iBufLength = rec->m_pMsg->m_iLength;

   memcpy(msg->m_pcBuffer, rec->m_pMsg->m_pcData, msg->m_iBufLength);

   delete rec->m_pMsg;
   delete rec;

   return msg->m_iBufLength;
}

int CGMP::recv(const int32_t& id, CUserMessage* msg)
{
   Sync::enterCS(m_ResQueueLock);

   map<int32_t, CMsgRecord*>::iterator m = m_mResQueue.find(id);

   if (m == m_mResQueue.end())
   {
      #ifndef WIN32
         timeval now;
         timespec timeout;
         gettimeofday(&now, 0);
         timeout.tv_sec = now.tv_sec + 1;
         timeout.tv_nsec = now.tv_usec * 1000;
         pthread_cond_timedwait(&m_ResQueueCond, &m_ResQueueLock, &timeout);
      #else
         ReleaseMutex(m_ResQueueLock);
         WaitForSingleObject(m_ResQueueCond, 1000);
         WaitForSingleObject(m_ResQueueLock, INFINITE);
      #endif

      m = m_mResQueue.find(id);
   }

   bool found = false;

   if (m != m_mResQueue.end())
   {
      if (msg->m_iBufLength < m->second->m_pMsg->m_iLength)
         msg->resize(m->second->m_pMsg->m_iLength);
      msg->m_iBufLength = m->second->m_pMsg->m_iLength;

      if (msg->m_iBufLength > 0)
         memcpy(msg->m_pcBuffer, m->second->m_pMsg->m_pcData, msg->m_iBufLength);

      delete m->second->m_pMsg;
      delete m->second;
      m_mResQueue.erase(m);

      found = true;
   }

   Sync::leaveCS(m_ResQueueLock);

   if (!found)
      return -1;

   return msg->m_iBufLength;
}

#ifndef WIN32
void* CGMP::sndHandler(void* s)
#else
DWORD WINAPI CGMP::sndHandler(LPVOID s)
#endif
{
   CGMP* self = (CGMP*)s;

   while (!self->m_bClosed)
   {
      #ifndef WIN32
         timespec timeout;
         timeval now;
         gettimeofday(&now, 0);
         timeout.tv_sec = now.tv_sec + 1;
         timeout.tv_nsec = now.tv_usec * 1000;
         pthread_mutex_lock(&self->m_SndQueueLock);
         pthread_cond_timedwait(&self->m_SndQueueCond, &self->m_SndQueueLock, &timeout);
         pthread_mutex_unlock(&self->m_SndQueueLock);
      #else
         WaitForSingleObject(self->m_SndQueueCond, 1000);
      #endif

      vector<CMsgRecord> udtsend;
      udtsend.clear();

      Sync::enterCS(self->m_SndQueueLock);
      for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end();)
      {
         if (Time::getTime() - (*i)->m_llTimeStamp > 10 * 1000000)
         {
            // timeout, send with UDT...
            list<CMsgRecord*>::iterator j = i;
            i ++;

            CMsgRecord rec;
            strcpy(rec.m_pcIP, (*j)->m_pcIP);
            rec.m_iPort = (*j)->m_iPort;
            rec.m_pMsg = new CGMPMessage(*((*j)->m_pMsg));
            udtsend.insert(udtsend.end(), rec);

            delete (*j)->m_pMsg;
            delete (*j);
            self->m_lSndQueue.erase(j);
            continue;
         }

         self->UDPsend((*i)->m_pcIP, (*i)->m_iPort, (*i)->m_pMsg);

         // check next msg
         ++ i;
      }
      Sync::leaveCS(self->m_SndQueueLock);

      for (vector<CMsgRecord>::iterator i = udtsend.begin(); i != udtsend.end(); ++ i)
      {
         self->UDTsend(i->m_pcIP, i->m_iPort, i->m_pMsg);
         delete i->m_pMsg;
      }
      udtsend.clear();
   }

   return NULL;
}

#ifndef WIN32
void* CGMP::rcvHandler(void* s)
#else
DWORD WINAPI CGMP::rcvHandler(LPVOID s)
#endif
{
   CGMP* self = (CGMP*)s;

   sockaddr_in addr;
   int32_t header[4];
   int32_t& type = header[0];
   int32_t& session = header[1];
   int32_t& id = header[2];
   int32_t& info = header[3];
   char* buf = new char [1456];

#ifndef WIN32
   iovec vec[2];
   vec[0].iov_base = header;
   vec[0].iov_len = 16;
   vec[1].iov_base = buf;
   vec[1].iov_len = 1456;

   msghdr mh;
   mh.msg_name = &addr;
   mh.msg_namelen = sizeof(sockaddr_in);
   mh.msg_iov = vec;
   mh.msg_iovlen = 2;
   mh.msg_control = NULL;
   mh.msg_controllen = 0;
   mh.msg_flags = 0;
#else
   WSABUF vec[2];
   vec[0].buf = (char*)header;
   vec[0].len = 16;
   vec[1].buf = buf;
   vec[1].len = 1456;
#endif

   int32_t ack[4];
   ack[0] = 1;
   ack[1] = CGMPMessage::g_iSession;
   ack[2] = 0;

   while (!self->m_bClosed)
   {
      #ifndef WIN32
         int rsize;
         if ((rsize = recvmsg(self->m_UDPSocket, &mh, 0)) < 0)
            continue;
      #else
         int asize = sizeof(sockaddr_in);
         DWORD rsize = 1472;
         DWORD flag = 0;

         if (0 != WSARecvFrom(self->m_UDPSocket, vec, 2, &rsize, &flag, (sockaddr*)&addr, &asize, NULL, NULL))
            continue;
      #endif

      if (type != 0)
      {
         switch (type)
         {
         case 1: // ACK
            Sync::enterCS(self->m_SndQueueLock);

            for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end(); ++ i)
            {
               if (id == (*i)->m_pMsg->m_iID)
               {
                  int rtt = Time::getTime() - (*i)->m_llTimeStamp;

                  #ifndef WIN32
                     char ip[64];
                     if (NULL != inet_ntop(AF_INET, &(addr.sin_addr), ip, 64))
                  #else
                     char* ip;
                     if (NULL != (ip = inet_ntoa(addr.sin_addr)))
                  #endif
                  self->m_PeerHistory.insert(ip, ntohs(addr.sin_port), session, -1, rtt);

                  #ifndef WIN32
                     pthread_cond_signal(&self->m_RTTCond);
                  #else
                     SetEvent(self->m_RTTCond);
                  #endif

                  delete (*i)->m_pMsg;
                  delete (*i);
                  self->m_lSndQueue.erase(i);
                  break;
               }
            }

            Sync::leaveCS(self->m_SndQueueLock);

            break;

         case 2: // RTT probe
            ack[2] = id;
            ack[3] = 0;
            ::sendto(self->m_UDPSocket, (char*)ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

            break;

         default:
            break;
         }

         continue;
      }

      // check repeated ID!!!!
      #ifndef WIN32
         char ip[64];
         if (NULL == inet_ntop(AF_INET, &(addr.sin_addr), ip, 64))
         {
            perror("inet_ntop");
            continue;
         }
      #else
         char* ip;
         if (NULL == (ip = inet_ntoa(addr.sin_addr)))
            continue;
      #endif
      int32_t lastid = self->m_PeerHistory.getLastID(ip, ntohs(addr.sin_port), session);

      if ((lastid >= 0) && (((id <= lastid) && (lastid - id < (1 << 29))) || ((id > lastid) && (id - lastid > (1 << 29)))))
      {
         ack[2] = id;
         ack[3] = 0;
         ::sendto(self->m_UDPSocket, (char*)ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

         continue;
      }

      CMsgRecord* rec = new CMsgRecord;

      #ifndef WIN32
         if (NULL == inet_ntop(AF_INET, &(addr.sin_addr), rec->m_pcIP, 64))
         {
            perror("inet_ntop");
            delete rec;
            continue;
         }
      #else
         char* tmp;
         if (NULL == (tmp = inet_ntoa(addr.sin_addr)))
         {
            //perror("inet_ntop");
            delete rec;
            continue;
         }
         strncpy(rec->m_pcIP, tmp, 64);
      #endif

      rec->m_iPort = ntohs(addr.sin_port);
      rec->m_pMsg = new CGMPMessage;
      //rec->m_pMsg->m_iType = type;
      rec->m_pMsg->m_iSession = session;
      rec->m_pMsg->m_iID = id;
      rec->m_pMsg->m_iInfo = info;
      rec->m_pMsg->m_iLength = rsize - 16;
      rec->m_pMsg->m_pcData = new char[rec->m_pMsg->m_iLength];
      memcpy(rec->m_pMsg->m_pcData, buf, rec->m_pMsg->m_iLength);

      self->m_PeerHistory.insert(rec->m_pcIP, rec->m_iPort, session, id);

      if (0 == info)
      {
         #ifndef WIN32
            pthread_mutex_lock(&self->m_RcvQueueLock);
            self->m_qRcvQueue.push(rec);
            pthread_mutex_unlock(&self->m_RcvQueueLock);
            pthread_cond_signal(&self->m_RcvQueueCond);
         #else
            WaitForSingleObject(self->m_RcvQueueLock, INFINITE);
            self->m_qRcvQueue.push(rec);
            ReleaseMutex(self->m_RcvQueueLock);
            SetEvent(self->m_RcvQueueCond);
         #endif
      }
      else
      {
         #ifndef WIN32
            pthread_mutex_lock(&self->m_ResQueueLock);
            self->m_mResQueue[info] = rec;
            pthread_mutex_unlock(&self->m_ResQueueLock);
            pthread_cond_signal(&self->m_ResQueueCond);
         #else
            WaitForSingleObject(self->m_ResQueueLock, INFINITE);
            self->m_mResQueue[info] = rec;
            ReleaseMutex(self->m_ResQueueLock);
            SetEvent(self->m_ResQueueCond);
         #endif
      }

      ack[2] = id;
      ack[3] = 0;
      ::sendto(self->m_UDPSocket, (char*)ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));
   }

   #ifndef WIN32
      pthread_cond_signal(&self->m_RcvQueueCond);
      pthread_cond_signal(&self->m_ResQueueCond);
   #else
      SetEvent(self->m_RcvQueueCond);
      SetEvent(self->m_ResQueueCond);
   #endif

   return NULL;
}

#ifndef WIN32
void* CGMP::udtRcvHandler(void* s)
#else
DWORD WINAPI CGMP::udtRcvHandler(LPVOID s)
#endif
{
   CGMP* self = (CGMP*)s;

   Transport t;
   sockaddr_in addr;
   int namelen = sizeof(sockaddr_in);

   int32_t header[4];

   while (!self->m_bClosed)
   {
      if (self->m_UDTSocket.accept(t, (sockaddr*)&addr, &namelen) < 0)
         continue;

      int port;
      if (t.recv((char*)&port, 4) < 0)
      {
         t.close();
         continue;
      }

      // recv "header" information
      if (t.recv((char*)header, 16) < 0)
      {
         t.close();
         continue;
      }

      CMsgRecord* rec = new CMsgRecord;

      #ifndef WIN32
         inet_ntop(AF_INET, &(addr.sin_addr), rec->m_pcIP, 64);
      #else
         char* tmp = inet_ntoa(addr.sin_addr);
         strncpy(rec->m_pcIP, tmp, 64);
      #endif

      rec->m_iPort = port;
      rec->m_pMsg = new CGMPMessage;
      //rec->m_pMsg->m_iType = type;
      rec->m_pMsg->m_iSession = header[1];
      rec->m_pMsg->m_iID = header[2];
      rec->m_pMsg->m_iInfo = header[3];

      // recv parameter size
      if (t.recv((char*)&(rec->m_pMsg->m_iLength), 4) < 0)
      {
         t.close();
         delete rec->m_pMsg;
         delete rec;
         continue;
      }

      rec->m_pMsg->m_pcData = new char[rec->m_pMsg->m_iLength];

      if (t.recv(rec->m_pMsg->m_pcData, rec->m_pMsg->m_iLength) < 0)
      {
         t.close();
         delete rec->m_pMsg;
         delete rec;
         continue;
      }

      t.close();

      self->m_PeerHistory.insert(rec->m_pcIP, rec->m_iPort, header[1], header[2]);

      if (0 == header[3])
      {
         #ifndef WIN32
            pthread_mutex_lock(&self->m_RcvQueueLock);
            self->m_qRcvQueue.push(rec);
            pthread_mutex_unlock(&self->m_RcvQueueLock);
            pthread_cond_signal(&self->m_RcvQueueCond);
         #else
            WaitForSingleObject(self->m_RcvQueueLock, INFINITE);
            self->m_qRcvQueue.push(rec);
            ReleaseMutex(self->m_RcvQueueLock);
            SetEvent(self->m_RcvQueueCond);
         #endif
      }
      else
      {
         #ifndef WIN32
            pthread_mutex_lock(&self->m_ResQueueLock);
            self->m_mResQueue[header[3]] = rec;
            pthread_mutex_unlock(&self->m_ResQueueLock);
            pthread_cond_signal(&self->m_ResQueueCond);
         #else
            WaitForSingleObject(self->m_ResQueueLock, INFINITE);
            self->m_mResQueue[header[3]] = rec;
            ReleaseMutex(self->m_ResQueueLock);
            SetEvent(self->m_ResQueueCond);
         #endif
      }
   }

   #ifndef WIN32
      pthread_cond_signal(&self->m_RcvQueueCond);
      pthread_cond_signal(&self->m_ResQueueCond);
   #else
      SetEvent(self->m_RcvQueueCond);
      SetEvent(self->m_ResQueueCond);
   #endif

   return NULL;
}

int CGMP::rpc(const char* ip, const int& port, CUserMessage* req, CUserMessage* res)
{
   int32_t id = 0;
   if (sendto(ip, port, id, req) < 0)
   {
      //cout << "RPC SND FAIL " << ip << " " << port << " " << req->m_iDataLength << *(int*)req->m_pcBuffer << " " << *(int*)(req->m_pcBuffer + 4) << endl;
      return -1;
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);

   while (recv(id, res) < 0)
   {
      if (rtt(ip, port, true) < 0)
      {
         //cout << "RPC RTT FAIL\n";
         return -1;
      }

      // 30 seconds maximum waiting time
      gettimeofday(&t2, 0);
      if (t2.tv_sec - t1.tv_sec > 30)
      {
         //cout << "RPC TIMEOUT \n";
         return -1;
      }
   }

   return 1;
}

int CGMP::rtt(const char* ip, const int& port, const bool& clear)
{
   if (!clear)
   {
      int r = m_PeerHistory.getRTT(ip);
      if (r > 0)
         return r;
   }
   else
      m_PeerHistory.clearRTT(ip);

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(2, 0);

   if (UDPsend(ip, port, msg) < 0)
   {
      delete msg;
      return -1;
   }

   CMsgRecord* rec = new CMsgRecord;
   strcpy(rec->m_pcIP, ip);
   rec->m_iPort = port;
   rec->m_pMsg = msg;
   rec->m_llTimeStamp = Time::getTime();

   Sync::enterCS(m_SndQueueLock);
   m_lSndQueue.push_back(rec);
   Sync::leaveCS(m_SndQueueLock);

   #ifndef WIN32
      timeval now;
      timespec timeout;
      gettimeofday(&now, 0);
      timeout.tv_sec = now.tv_sec + 1;
      timeout.tv_nsec = now.tv_usec * 1000;
      pthread_mutex_lock(&m_RTTLock);
      pthread_cond_timedwait(&m_RTTCond, &m_RTTLock, &timeout);
      pthread_mutex_unlock(&m_RTTLock);
   #else
      WaitForSingleObject(m_RTTCond, 1000);
   #endif

   return m_PeerHistory.getRTT(ip);
}
