/*****************************************************************************
Copyright (c) 2005 - 2011, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu, last updated 04/19/2011
*****************************************************************************/


#ifndef WIN32
   #include <unistd.h>
   #include <stdio.h>
   #include <errno.h>
   #include <assert.h>
   #include <netdb.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif

#include <sstream>
#include <common.h>
#include <gmp.h>

using namespace std;


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

   CGuard::enterCS(g_IDLock);

   m_iID = g_iID ++;
   if (g_iID == g_iMaxID)
      g_iID = 1;

   CGuard::leaveCS(g_IDLock);
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
   delete [] m_pcData;
}

void CGMPMessage::pack(const char* data, const int& len, const int32_t& info)
{
   m_iType = 0;

   if (len > 0)
   {
      delete [] m_pcData;
      m_pcData = new char[len];
      memcpy(m_pcData, data, len);
      m_iLength = len;
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
   srand(CTimer::getTime());
   int32_t r = rand();
   while (r == 0)
      r = rand();
   return r;
}


CGMP::CGMP()
{
   CGuard::createMutex(m_SndQueueLock);
   CGuard::createCond(m_SndQueueCond);
   CGuard::createMutex(m_RcvQueueLock);
   CGuard::createCond(m_RcvQueueCond);
   CGuard::createMutex(m_ResQueueLock);
   CGuard::createCond(m_ResQueueCond);
   CGuard::createMutex(m_RTTLock);
   CGuard::createCond(m_RTTCond);

   m_bInit = false;
   m_bClosed = false;
}

CGMP::~CGMP()
{
   CGuard::releaseMutex(m_SndQueueLock);
   CGuard::releaseCond(m_SndQueueCond);
   CGuard::releaseMutex(m_RcvQueueLock);
   CGuard::releaseCond(m_RcvQueueCond);
   CGuard::releaseMutex(m_ResQueueLock);
   CGuard::releaseCond(m_ResQueueCond);
   CGuard::releaseMutex(m_RTTLock);
   CGuard::releaseCond(m_RTTCond);
}

int CGMP::init(const int& port)
{
   UDT::startup();

   if (port != 0)
      m_iPort = port;
   else
      m_iPort = 0;

   addrinfo hints;
   addrinfo* res;
   memset(&hints, 0, sizeof(addrinfo));

   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_DGRAM;

   stringstream service;
   service << port;

   if (0 != getaddrinfo(NULL, service.str().c_str(), &hints, &res))
      return -1;     

   m_UDPSocket = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
   if (0 != ::bind(m_UDPSocket, res->ai_addr, res->ai_addrlen))
      return -1;

   if (port == 0)
   {
      // retrieve the UDP port if user doesn't specify one
      ::getsockname(m_UDPSocket, res->ai_addr, &res->ai_addrlen);
      char portbuf[NI_MAXSERV];
      ::getnameinfo(res->ai_addr, res->ai_addrlen, NULL, 0, portbuf, sizeof(portbuf), NI_NUMERICSERV);
      m_iPort = atoi(portbuf);
   }

   freeaddrinfo(res);

   // recv() is timed, avoid infinite block
   timeval tv;
   tv.tv_sec = 0;
   tv.tv_usec = 10000;
   ::setsockopt(m_UDPSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(timeval));

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

   return 0;
}

int CGMP::close()
{
   if (!m_bInit)
      return 0;

   m_bClosed = true;

   #ifndef WIN32
      ::close(m_UDPSocket);

      pthread_mutex_lock(&m_SndQueueLock);
      pthread_cond_signal(&m_SndQueueCond);
      pthread_mutex_unlock(&m_SndQueueLock);

      pthread_join(m_SndThread, NULL);
      pthread_join(m_RcvThread, NULL);
      pthread_join(m_UDTRcvThread, NULL);
   #else
      ::closesocket(m_UDPSocket);

      SetEvent(m_SndQueueCond);
      WaitForSingleObject(m_SndThread, INFINITE);
      WaitForSingleObject(m_RcvThread, INFINITE);
      WaitForSingleObject(m_UDTRcvThread, INFINITE);
   #endif

   UDT::cleanup();

   return 0;
}

int CGMP::getPort()
{
   return m_iPort;
}

int CGMP::sendto(const string& ip, const int& port, int32_t& id, const CUserMessage* msg)
{
   if (msg->m_iDataLength <= m_iMaxUDPMsgSize)
      return UDPsend(ip.c_str(), port, id, msg->m_pcBuffer, msg->m_iDataLength, true);

   return UDTsend(ip.c_str(), port, id, msg->m_pcBuffer, msg->m_iDataLength);
}

int CGMP::UDPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable)
{
   m_PeerHistory.flowControl(ip, port, CGMPMessage::g_iSession);

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(data, len, id);
   id = msg->m_iID;

   int res = UDPsend(ip, port, msg);
   if (res < 0)
      return -1;

   if (reliable)
   {
      CMsgRecord* rec = new CMsgRecord;
      rec->m_strIP = ip;
      rec->m_iPort = port;
      rec->m_pMsg = msg;
      rec->m_llTimeStamp = CTimer::getTime();

      CGuard::enterCS(m_SndQueueLock);
      m_lSndQueue.push_back(rec);
      CGuard::leaveCS(m_SndQueueLock);
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
   UDTSOCKET usock;
   if (m_PeerHistory.getUDTSocket(ip, port, usock) < 0)
   {
      CGMPMessage ctrl_msg;
      ctrl_msg.pack(3, m_iUDTReusePort);
      UDPsend(ip, port, &ctrl_msg);

      CMsgRecord* rec = new CMsgRecord;
      rec->m_strIP = ip;
      rec->m_iPort = port;
      rec->m_pMsg = &ctrl_msg;
      rec->m_llTimeStamp = CTimer::getTime();

      CGuard::enterCS(m_SndQueueLock);
      m_lSndQueue.push_back(rec);
      CGuard::leaveCS(m_SndQueueLock);

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

      // get UDT port

      if ((UDTCreate(usock) < 0) || (UDTConnect(usock, ip, port)) < 0)
         return -1;

      UDT::epoll_add_usock(m_iUDTEPollID, usock);

      m_PeerHistory.setUDTSocket(ip, port, usock);
   }

   // now UDT connection is ready, send data
   CGMPMessage msg;
   msg.pack(data, len, id);
   id = msg.m_iID;
   int res = UDTsend(ip, port, &msg);

   return res;
}

int CGMP::UDTsend(const char* ip, const int& port, CGMPMessage* msg)
{
   // locate cached UDT socket
   UDTSOCKET usock;
   if (m_PeerHistory.getUDTSocket(ip, port, usock) < 0)
      return -1;

   if ((UDTSend(usock, (char*)(&m_iPort), 4) < 0) || (UDTSend(usock, (char*)(msg->m_piHeader), 16) < 0) || (UDTSend(usock, (char*)&(msg->m_iLength), 4) < 0))
   {
      return -1;
   }

   if (UDTSend(usock, msg->m_pcData, msg->m_iLength) < 0)
   {
      return -1;
   }

   return 16 + msg->m_iLength;
}

int CGMP::UDTCreate(UDTSOCKET& usock)
{
   addrinfo hints;
   addrinfo* res;

   memset(&hints, 0, sizeof(struct addrinfo));
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;

   // UDT uses GMP port - 1
   stringstream service;
   service << m_iUDTReusePort;
   if (0 != getaddrinfo(NULL, service.str().c_str(), &hints, &res))
      return -1;

   usock = UDT::socket(res->ai_family, res->ai_socktype, res->ai_protocol);

   bool reuse = true;
   UDT::setsockopt(usock, 0, UDT_REUSEADDR, &reuse, sizeof(bool));
   bool rendezvous = true;
   UDT::setsockopt(usock, 0, UDT_RENDEZVOUS, &rendezvous, sizeof(bool));

   if (UDT::ERROR == UDT::bind(usock, res->ai_addr, res->ai_addrlen))
      return -1;

   freeaddrinfo(res);

   return 0;
}

int CGMP::UDTConnect(const UDTSOCKET& usock, const char* ip, const int& port)
{
   addrinfo hints;
   addrinfo* peer;

   memset(&hints, 0, sizeof(struct addrinfo));
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;

   stringstream service;
   service << port;
   if (0 != getaddrinfo(ip, service.str().c_str(), &hints, &peer))
      return -1;

   if (UDT::ERROR == UDT::connect(usock, peer->ai_addr, peer->ai_addrlen))
      return -1;

   freeaddrinfo(peer);

   return 0;
}

int CGMP::UDTSend(const UDTSOCKET& usock, const char* buf, const int& size)
{
   return 0;
}

int CGMP::UDTRecv(const UDTSOCKET& usock, const char* buf, const int& size)
{
   return 0;
}

int CGMP::recvfrom(string& ip, int& port, int32_t& id, CUserMessage* msg, const bool& block)
{
   bool timeout = false;

   CGuard::enterCS(m_RcvQueueLock);

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
      CGuard::leaveCS(m_RcvQueueLock);
      return -1;
   }

   CMsgRecord* rec = m_qRcvQueue.front();
   m_qRcvQueue.pop();

   CGuard::leaveCS(m_RcvQueueLock);

   ip = rec->m_strIP;
   port = rec->m_iPort;
   id = rec->m_pMsg->m_iID;

   if (msg->m_iBufLength < rec->m_pMsg->m_iLength)
      msg->resize(rec->m_pMsg->m_iLength);
   msg->m_iDataLength = rec->m_pMsg->m_iLength;

   memcpy(msg->m_pcBuffer, rec->m_pMsg->m_pcData, msg->m_iDataLength);

   delete rec->m_pMsg;
   delete rec;

   return msg->m_iDataLength;
}

int CGMP::recv(const int32_t& id, CUserMessage* msg)
{
   CGuard::enterCS(m_ResQueueLock);

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
      msg->m_iDataLength = m->second->m_pMsg->m_iLength;

      if (msg->m_iDataLength > 0)
         memcpy(msg->m_pcBuffer, m->second->m_pMsg->m_pcData, msg->m_iDataLength);

      delete m->second->m_pMsg;
      delete m->second;
      m_mResQueue.erase(m);

      found = true;
   }

   CGuard::leaveCS(m_ResQueueLock);

   if (!found)
      return -1;

   return msg->m_iDataLength;
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

      vector<CMsgRecord*> udtsend;
      udtsend.clear();

      CGuard::enterCS(self->m_SndQueueLock);

      int64_t ts = CTimer::getTime();

      for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end();)
      {
         int64_t diff = ts - (*i)->m_llTimeStamp;

         if (diff > 10 * 1000000)
         {
            // timeout, send with UDT...
            list<CMsgRecord*>::iterator j = i;
            i ++;
            udtsend.push_back(*j);
            self->m_lSndQueue.erase(j);
            continue;
         }
         else if (diff > 1000000)
            self->UDPsend((*i)->m_strIP.c_str(), (*i)->m_iPort, (*i)->m_pMsg);

         // check next msg
         ++ i;
      }

      CGuard::leaveCS(self->m_SndQueueLock);

      for (vector<CMsgRecord*>::iterator i = udtsend.begin(); i != udtsend.end(); ++ i)
      {
         // use UDT for data only
         if ((*i)->m_pMsg->m_piHeader[0] == 0)
            self->UDTsend((*i)->m_strIP.c_str(), (*i)->m_iPort, (*i)->m_pMsg);
         delete (*i)->m_pMsg;
         delete (*i);
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
   char* buf = new char [m_iMaxUDPMsgSize];

#ifndef WIN32
   iovec vec[2];
   vec[0].iov_base = header;
   vec[0].iov_len = 16;
   vec[1].iov_base = buf;
   vec[1].iov_len = m_iMaxUDPMsgSize;

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
   vec[1].len = m_iMaxUDPMsgSize;
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
            CGuard::enterCS(self->m_SndQueueLock);

            for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end(); ++ i)
            {
               if (id == (*i)->m_pMsg->m_iID)
               {
                  int rtt = int(CTimer::getTime() - (*i)->m_llTimeStamp);

                  #ifndef WIN32
                     char ip[64];
                     if (NULL != inet_ntop(AF_INET, &(addr.sin_addr), ip, 64))
                  #else
                     char* ip;
                     if (NULL != (ip = inet_ntoa(addr.sin_addr)))
                  #endif
                     self->m_PeerHistory.insert(ip, ntohs(addr.sin_port), CGMPMessage::g_iSession, -1, rtt, info);

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

            CGuard::leaveCS(self->m_SndQueueLock);

            break;

         case 2: // RTT probe
            ack[2] = id;
            ack[3] = 0;
            ::sendto(self->m_UDPSocket, (char*)ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

            break;

         case 3: // rendezvous UDT connection request
            ack[2] = id;
            ack[3] = self->m_iUDTReusePort;
            ::sendto(self->m_UDPSocket, (char*)ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

            // check if connection already exist
            //if (self->m_PeerHistory.getUDTSocket())
            //  break;

            // check existing UDT socket
            // if not exist do asynchronous rendezvous connect
            // insert to connection cache
            char ip[NI_MAXHOST];
            char port[NI_MAXSERV];
            getnameinfo((sockaddr*)&addr, sizeof(sockaddr_in), ip, sizeof(ip), port, sizeof(port), NI_NUMERICHOST|NI_NUMERICSERV);

            // TODO: add IPv6 support

            UDTSOCKET usock;
            if ((self->UDTCreate(usock) >= 0) && (self->UDTConnect(usock, ip, atoi(port))) >= 0)
            {
               UDT::epoll_add_usock(self->m_iUDTEPollID, usock);
               self->m_PeerHistory.setUDTSocket(ip, atoi(port), usock);
            }

            break;

         default:
            break;
         }

         continue;
      }

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

      // repeated message, send ACK and disgard
      if (self->m_PeerHistory.hit(ip, ntohs(addr.sin_port), session, id))
      {
         ack[2] = id;
         ack[3] = 0;
         ::sendto(self->m_UDPSocket, (char*)ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

         continue;
      }

      CMsgRecord* rec = new CMsgRecord;
      rec->m_strIP = ip;
      rec->m_iPort = ntohs(addr.sin_port);
      rec->m_pMsg = new CGMPMessage;
      //rec->m_pMsg->m_iType = type;
      rec->m_pMsg->m_iSession = session;
      rec->m_pMsg->m_iID = id;
      rec->m_pMsg->m_iInfo = info;
      rec->m_pMsg->m_iLength = rsize - 16;
      rec->m_pMsg->m_pcData = new char[rec->m_pMsg->m_iLength];
      memcpy(rec->m_pMsg->m_pcData, buf, rec->m_pMsg->m_iLength);

      self->m_PeerHistory.insert(rec->m_strIP, rec->m_iPort, session, id);

      int qsize = 0;

      if (0 == info)
      {
         #ifndef WIN32
            pthread_mutex_lock(&self->m_RcvQueueLock);
            self->m_qRcvQueue.push(rec);
            qsize += self->m_qRcvQueue.size();
            pthread_mutex_unlock(&self->m_RcvQueueLock);
            pthread_cond_signal(&self->m_RcvQueueCond);
         #else
            WaitForSingleObject(self->m_RcvQueueLock, INFINITE);
            self->m_qRcvQueue.push(rec);
            qsize += self->m_qRcvQueue.size();
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
      ack[3] = qsize; // flow control
      ::sendto(self->m_UDPSocket, (char*)ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));
   }

   delete [] buf;

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

   int32_t header[4];

   while (!self->m_bClosed)
   {
      set<UDTSOCKET> readfds;
      UDT::epoll_wait(self->m_iUDTEPollID, &readfds, NULL, -1);

      for (set<UDTSOCKET>::iterator i = readfds.begin(); i != readfds.end(); ++ i)
      {
         int port;
         if (self->UDTRecv(*i, (char*)&port, 4) < 0)
            continue;

         // recv "header" information
         if (self->UDTRecv(*i, (char*)header, 16) < 0)
            continue;

         sockaddr_in addr;
         int addrlen = sizeof(sockaddr_in);
         UDT::getpeername(*i, (sockaddr*)&addr, &addrlen);
         char peer_ip[NI_MAXHOST];
         char peer_udt_port[NI_MAXSERV];
         getnameinfo((sockaddr*)&addr, addrlen, peer_ip, sizeof(peer_ip), peer_udt_port, sizeof(peer_udt_port), NI_NUMERICHOST|NI_NUMERICSERV);

         CMsgRecord* rec = new CMsgRecord;

         rec->m_strIP = peer_ip;
         rec->m_iPort = port;
         rec->m_pMsg = new CGMPMessage;
         //rec->m_pMsg->m_iType = type;
         rec->m_pMsg->m_iSession = header[1];
         rec->m_pMsg->m_iID = header[2];
         rec->m_pMsg->m_iInfo = header[3];

         // recv parameter size
         if (self->UDTRecv(*i, (char*)&(rec->m_pMsg->m_iLength), 4) < 0)
         {
            delete rec->m_pMsg;
            delete rec;
            continue;
         }

         rec->m_pMsg->m_pcData = new char[rec->m_pMsg->m_iLength];

         if (self->UDTRecv(*i, rec->m_pMsg->m_pcData, rec->m_pMsg->m_iLength) < 0)
         {
            delete rec->m_pMsg;
            delete rec;
            continue;
         }

         if (self->m_PeerHistory.hit(rec->m_strIP, rec->m_iPort, rec->m_pMsg->m_iSession, rec->m_pMsg->m_iID))
            continue;

         self->m_PeerHistory.insert(rec->m_strIP, rec->m_iPort, rec->m_pMsg->m_iSession, rec->m_pMsg->m_iID);

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

int CGMP::rpc(const string& ip, const int& port, CUserMessage* req, CUserMessage* res)
{
   int32_t id = 0;
   if (sendto(ip, port, id, req) < 0)
      return -1;

   uint64_t t = CTimer::getTime();
   int errcount = 0;

   while (recv(id, res) < 0)
   {
      if (rtt(ip, port, true) < 0)
         errcount ++;

      if (errcount > 10)
         return -1;

      // 60 seconds maximum waiting time
      if (CTimer::getTime() - t > 60000000)
         return -1;
   }

   return 0;
}

int CGMP::multi_rpc(const vector<Address>& dest, CUserMessage* req, vector<CUserMessage*>* res)
{
   unsigned int tn = dest.size();

   if (0 == tn)
      return 0;

   if ((NULL != res) && (res->size() != tn))
      return -1;

   vector<int> ids;
   ids.resize(tn);
   vector<int>::iterator n = ids.begin();
   for (vector<Address>::const_iterator i = dest.begin(); i != dest.end(); ++ i)
   {
      int id = 0;       
      if (sendto(i->m_strIP, i->m_iPort, id, req) < 0)
         id = 0;

      *n = id;
      ++ n;
   }

   vector<CUserMessage*>::iterator m;
   if (NULL != res) 
      m = res->begin();
   n = ids.begin();
   vector<Address>::const_iterator a = dest.begin();
   uint64_t start_time = CTimer::getTime();
   int fail_num = tn;

   for (; n != ids.end(); ++ n)
   {
      if (0 != *n)
      {
         int errcount = 0;
         bool found = true;
         CUserMessage tmp;
         CUserMessage* msg;
         if ((NULL != res) && (NULL != *m))
            msg = *m;
         else
            msg = &tmp;

         while (recv(*n, msg) < 0)
         {
            if (rtt(a->m_strIP, a->m_iPort, true) < 0)
               errcount ++;

            // 60 seconds maximum waiting time
            if ((errcount > 10) || (CTimer::getTime() - start_time > 60000000))
            {
               msg->m_iDataLength = 0;
               found = false;
               break;
            }
         }

         if (found)
            fail_num --;
      }

      if (NULL != res)
         ++ m;
      ++ a;
   }

   return -fail_num;
}

int CGMP::rtt(const string& ip, const int& port, const bool& clear)
{
   if (!clear)
   {
      int r = m_PeerHistory.getRTT(ip);
      if (r > 0)
         return r;
   }
   else
   {
      m_PeerHistory.clearRTT(ip);
   }

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(2, 0);

   if (UDPsend(ip.c_str(), port, msg) < 0)
   {
      delete msg;
      return -1;
   }

   CMsgRecord* rec = new CMsgRecord;
   rec->m_strIP = ip;
   rec->m_iPort = port;
   rec->m_pMsg = msg;
   rec->m_llTimeStamp = CTimer::getTime();

   CGuard::enterCS(m_SndQueueLock);
   m_lSndQueue.push_back(rec);
   CGuard::leaveCS(m_SndQueueLock);

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
