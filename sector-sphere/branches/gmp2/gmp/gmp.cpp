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
   #include <assert.h>
   #include <errno.h>
   #include <netdb.h>
   #include <stdio.h>
   #include <unistd.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif
#include <sstream>

#include "common.h"
#include "gmp.h"

#include <iostream>
using namespace std;

int32_t CGMPMessage::g_iSession = CGMPMessage::initSession();
int32_t CGMPMessage::g_iID = 1;
const int CGMPMessage::g_iHdrField = 6;
const int CGMPMessage::g_iHdrSize = 24;
#ifndef WIN32
   pthread_mutex_t CGMPMessage::g_IDLock = PTHREAD_MUTEX_INITIALIZER;
#else
   pthread_mutex_t CGMPMessage::g_IDLock = CreateMutex(NULL, false, NULL);
#endif

CGMPMessage::CGMPMessage():
m_iType(m_piHeader[0]),
m_iSession(m_piHeader[1]),
m_iSrcChn(m_piHeader[2]),
m_iDstChn(m_piHeader[3]),
m_iID(m_piHeader[4]),
m_iInfo(m_piHeader[5]),
m_pcData(NULL),
m_iLength(0)
{
   memset(m_piHeader, g_iHdrSize * sizeof(int32_t), 0);

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
m_iSrcChn(m_piHeader[2]),
m_iDstChn(m_piHeader[3]),
m_iID(m_piHeader[4]),
m_iInfo(m_piHeader[5]),
m_pcData(NULL),
m_iLength(0)
{
   memcpy((char*)m_piHeader, (char*)msg.m_piHeader, g_iHdrSize);
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

void CGMPMessage::pack(const char* data, const int& len, const int32_t& info, const int& src_chn, const int& dst_chn)
{
   m_iType = 0;
   m_iSrcChn = src_chn;
   m_iDstChn = dst_chn;

   delete [] m_pcData;
   if (len > 0)
   {
      m_pcData = new char[len];
      memcpy(m_pcData, data, len);
      m_iLength = len;
   }
   else
      m_iLength = 0;

   m_iInfo = info;
}

void CGMPMessage::pack(const int32_t& type, const int32_t& info, const int& src_chn, const int& dst_chn)
{
   delete [] m_pcData;
   m_iLength = 0;

   m_iType = type;
   m_iSrcChn = src_chn;
   m_iDstChn = dst_chn;
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

CChannelRec::CChannelRec()
{
   CGuard::createMutex(m_RcvQueueLock);
   CGuard::createCond(m_RcvQueueCond);
   CGuard::createMutex(m_ResQueueLock);
   CGuard::createCond(m_ResQueueCond);
}

CChannelRec::~CChannelRec()
{
   CGuard::releaseMutex(m_RcvQueueLock);
   CGuard::releaseCond(m_RcvQueueCond);
   CGuard::releaseMutex(m_ResQueueLock);
   CGuard::releaseCond(m_ResQueueCond);
}

CGMP::CGMP():
m_iUDTReusePort(0),
m_bInit(false),
m_bClosed(false),
m_iChnIDSeed(0)
{
   CGuard::createMutex(m_SndQueueLock);
   CGuard::createCond(m_SndQueueCond);
   CGuard::createMutex(m_RTTLock);
   CGuard::createCond(m_RTTCond);
   CGuard::createMutex(m_ChnLock);
}

CGMP::~CGMP()
{
   //TODO: clear all un-received messages.

   CGuard::releaseMutex(m_SndQueueLock);
   CGuard::releaseCond(m_SndQueueCond);
   CGuard::releaseMutex(m_RTTLock);
   CGuard::releaseCond(m_RTTCond);
   CGuard::releaseMutex(m_ChnLock);
}

int CGMP::init(const int& port)
{
   UDT::startup();

   m_iPort = port; 

   // create the UDP socket for small messages.
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

   // This UDT socket is not used for data transfer. We need this to
   // keep a permanent UDP port sharing for all UDT sockets for large messages.
   UDTCreate(m_UDTSocket);
   m_iUDTEPollID = UDT::epoll_create();

   // recv() is timed, avoid infinite block.
   timeval tv;
   tv.tv_sec = 0;
   tv.tv_usec = 10000;
   ::setsockopt(m_UDPSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(timeval));

   // Channel 0 always exists.
   m_mCurrentChn[0] = new CChannelRec;

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

   m_iUDTReusePort = 0;
   m_bInit = false;
   m_bClosed = true;
   UDT::epoll_release(m_iUDTEPollID);

   m_lSndQueue.clear();
   //
   //while (!m_qRcvQueue.empty())
   //   m_qRcvQueue.pop();
   //m_mResQueue.clear();
   m_PeerHistory.clear();

   // Release all channels.
   CGuard::enterCS(m_ChnLock);
   for (map<int, CChannelRec*>::iterator i = m_mCurrentChn.begin(); i != m_mCurrentChn.end(); ++ i)
      delete i->second;
   m_mCurrentChn.clear();
   CGuard::leaveCS(m_ChnLock);

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

   // Reset this flag so that the GMP is completedly reset to initial state.
   // It can be init() again.
   m_bClosed = false;
   return 0;
}

int CGMP::getPort()
{
   return m_iPort;
}

int CGMP::sendto(const string& ip, const int& port, int32_t& id, const CUserMessage* msg, const int& src_chn, const int& dst_chn)
{
   if (msg->m_iDataLength <= m_iMaxUDPMsgSize)
      return UDPsend(ip.c_str(), port, id, src_chn, dst_chn, msg->m_pcBuffer, msg->m_iDataLength, true);

   return UDTsend(ip.c_str(), port, id, src_chn, dst_chn, msg->m_pcBuffer, msg->m_iDataLength);
}

int CGMP::UDPsend(const char* ip, const int& port, int32_t& id, const int& src_chn, const int& dst_chn,
                  const char* data, const int& len, const bool& reliable)
{
   // If the destination has too many messages in queue, block for a while.
   m_PeerHistory.flowControl(ip, port, CGMPMessage::g_iSession);

   // TODO: add snd buffer control, block if there are too many messages in the snd queue.
   // use cache structure for the snd queue.

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(data, len, id, src_chn, dst_chn);
   id = msg->m_iID;

   int res = 0;

   if (reliable)
   {
      // TODO: use a message pool, rather then new each time.
      CMsgRecord* rec = new CMsgRecord;
      rec->m_strIP = ip;
      rec->m_iPort = port;
      rec->m_pMsg = msg;
      rec->m_llTimeStamp = CTimer::getTime();

      CGuard::enterCS(m_SndQueueLock);
      m_lSndQueue.push_back(rec);
      CGuard::leaveCS(m_SndQueueLock);

      if (UDPsend(ip, port, msg) < 0)
         return -1;
   }
   else
   {
      if (UDPsend(ip, port, msg) < 0)
         return -1;
      delete msg;
   }

   return res;
}

int CGMP::UDPsend(const char* ip, const int& port, CGMPMessage* msg)
{
   // TODO: pack this into a common routine.
   // TODO: probably don't need to convert every time.
   addrinfo hints;
   addrinfo* res;
   memset(&hints, 0, sizeof(addrinfo));
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_DGRAM;
   stringstream service;
   service << port;
   if (0 != getaddrinfo(ip, service.str().c_str(), &hints, &res))
      return -1;

   #ifndef WIN32
      iovec vec[2];
      vec[0].iov_base = msg->m_piHeader;
      vec[0].iov_len = CGMPMessage::g_iHdrSize;
      vec[1].iov_base = msg->m_pcData;
      vec[1].iov_len = msg->m_iLength;

      msghdr mh;
      mh.msg_name = res->ai_addr;
      mh.msg_namelen = res->ai_addrlen;
      mh.msg_iov = vec;
      mh.msg_iovlen = 2;
      mh.msg_control = NULL;
      mh.msg_controllen = 0;
      mh.msg_flags = 0;
      sendmsg(m_UDPSocket, &mh, 0);
   #else
      WSABUF vec[2];
      vec[0].buf = (char*)msg->m_piHeader;
      vec[0].len = CGMPMessage::g_iHdrSize;
      vec[1].buf = msg->m_pcData;
      vec[1].len = msg->m_iLength;

      DWORD ssize;
      WSASendTo(m_UDPSocket, vec, 2, &ssize, 0, res->ai_addr, res->ai_addrlen, NULL, NULL);
   #endif

   freeaddrinfo(res);

   return msg->m_iLength + CGMPMessage::g_iHdrSize;
}

int CGMP::UDTsend(const char* ip, const int& port, int32_t& id, const int& src_chn, const int& dst_chn,
                  const char* data, const int& len)
{
   UDTSOCKET usock;
   if ((m_PeerHistory.getUDTSocket(ip, port, usock) < 0) ||
       (UDT::getsockstate(usock) != CONNECTED))
   {
      if (UDTCreate(usock) < 0)
         return -1;

      UDT::epoll_add_usock(m_iUDTEPollID, usock);
      m_PeerHistory.setUDTSocket(ip, port, usock);

      // Store the message for sending when the UDT connection is set up.
      CGMPMessage* msg = new CGMPMessage;
      msg->pack(data, len, id, src_chn, dst_chn);
      CMsgRecord* rec = new CMsgRecord;
      rec->m_strIP = ip;
      rec->m_iPort = port;
      rec->m_pMsg = msg;
      rec->m_llTimeStamp = CTimer::getTime();
      CGuard::enterCS(m_SndQueueLock);
      m_lSndQueue.push_back(rec);
      CGuard::leaveCS(m_SndQueueLock);

      // Request the destination GMP to set up a UDT connection.
      CGMPMessage* ctrl_msg = new CGMPMessage;
      ctrl_msg->pack(3, m_iUDTReusePort);
      CMsgRecord* ctrl_rec = new CMsgRecord;
      ctrl_rec->m_strIP = ip;
      ctrl_rec->m_iPort = port;
      ctrl_rec->m_pMsg = ctrl_msg;
      rec->m_llTimeStamp = CTimer::getTime();
      CGuard::enterCS(m_SndQueueLock);
      m_lSndQueue.push_back(ctrl_rec);
      CGuard::leaveCS(m_SndQueueLock);

      UDPsend(ip, port, ctrl_msg);

      return 0;
   }

   // UDT connection is ready, send data immediately
   CGMPMessage msg;
   msg.pack(data, len, id, src_chn, dst_chn);
   id = msg.m_iID;
   return UDTsend(ip, port, &msg);
}

int CGMP::UDTsend(const char* ip, const int& port, CGMPMessage* msg)
{
   // locate cached UDT socket
   UDTSOCKET usock;
   if (m_PeerHistory.getUDTSocket(ip, port, usock) < 0)
      return -1;

   if (UDT::getsockstate(usock) != CONNECTED)
      return -1;

   if ((UDTSend(usock, (char*)(&m_iPort), 4) < 0) ||
       (UDTSend(usock, (char*)(msg->m_piHeader), CGMPMessage::g_iHdrSize) < 0) ||
       (UDTSend(usock, (char*)&(msg->m_iLength), 4) < 0))
      return -1;

   if (UDTSend(usock, msg->m_pcData, msg->m_iLength) < 0)
      return -1;

   return msg->m_iLength + CGMPMessage::g_iHdrSize;
}

int CGMP::recvfrom(string& ip, int& port, int32_t& id, CUserMessage* msg, const bool& block,
                   int* src_chn, const int& dst_chn)
{
   CChannelRec* chn = getChnHandle(dst_chn);
   // Channel does not exist, return error.
   if (NULL == chn)
      return -1;

   bool timeout = false;

   CGuard::enterCS(chn->m_RcvQueueLock);

   while (!m_bClosed && chn->m_qRcvQueue.empty() && !timeout)
   {
      #ifndef WIN32
         if (block)
            pthread_cond_wait(&chn->m_RcvQueueCond, &chn->m_RcvQueueLock);
         else
         {
            timeval now;
            timespec expiretime;
            gettimeofday(&now, 0);
            expiretime.tv_sec = now.tv_sec + 1;
            expiretime.tv_nsec = now.tv_usec * 1000;
            if (pthread_cond_timedwait(&chn->m_RcvQueueCond, &chn->m_RcvQueueLock, &expiretime) != 0)
               timeout = true;
         }
      #else
         ReleaseMutex(m_RcvQueueLock);
         if (block)
            WaitForSingleObject(chn->m_RcvQueueCond, INFINITE);
         else
         {
            if (WaitForSingleObject(chn->m_RcvQueueCond, 1000) == WAIT_TIMEOUT)
               timeout = true;
         }
         WaitForSingleObject(chn->m_RcvQueueLock, INFINITE);
      #endif
   }

   if (m_bClosed || timeout)
   {
      CGuard::leaveCS(chn->m_RcvQueueLock);
      return -1;
   }

   CMsgRecord* rec = chn->m_qRcvQueue.front();
   chn->m_qRcvQueue.pop();

   CGuard::leaveCS(chn->m_RcvQueueLock);

   ip = rec->m_strIP;
   port = rec->m_iPort;
   id = rec->m_pMsg->m_iID;
   if (src_chn)
      *src_chn = rec->m_pMsg->m_iSrcChn;

   if (msg->m_iBufLength < rec->m_pMsg->m_iLength)
      msg->resize(rec->m_pMsg->m_iLength);
   msg->m_iDataLength = rec->m_pMsg->m_iLength;

   memcpy(msg->m_pcBuffer, rec->m_pMsg->m_pcData, msg->m_iDataLength);

   delete rec->m_pMsg;
   delete rec;

   return msg->m_iDataLength;
}

int CGMP::recv(const int32_t& id, CUserMessage* msg, int* src_chn, const int& dst_chn)
{
   CChannelRec* chn = getChnHandle(dst_chn);
   // Channel does not exist, return error.
   if (NULL == chn)
      return -1;

   CGuard::enterCS(chn->m_ResQueueLock);

   map<int32_t, CMsgRecord*>::iterator m = chn->m_mResQueue.find(id);

   if (m == chn->m_mResQueue.end())
   {
      #ifndef WIN32
         timeval now;
         timespec timeout;
         gettimeofday(&now, 0);
         timeout.tv_sec = now.tv_sec + 1;
         timeout.tv_nsec = now.tv_usec * 1000;
         pthread_cond_timedwait(&chn->m_ResQueueCond, &chn->m_ResQueueLock, &timeout);
      #else
         ReleaseMutex(chn->m_ResQueueLock);
         WaitForSingleObject(chn->m_ResQueueCond, 1000);
         WaitForSingleObject(chn->m_ResQueueLock, INFINITE);
      #endif

      m = chn->m_mResQueue.find(id);
   }

   bool found = false;

   if (m != chn->m_mResQueue.end())
   {
      if (msg->m_iBufLength < m->second->m_pMsg->m_iLength)
         msg->resize(m->second->m_pMsg->m_iLength);
      msg->m_iDataLength = m->second->m_pMsg->m_iLength;

      if (msg->m_iDataLength > 0)
         memcpy(msg->m_pcBuffer, m->second->m_pMsg->m_pcData, msg->m_iDataLength);

      if (src_chn)
         *src_chn = m->second->m_pMsg->m_iSrcChn;

      delete m->second->m_pMsg;
      delete m->second;
      chn->m_mResQueue.erase(m);

      found = true;
   }

   CGuard::leaveCS(chn->m_ResQueueLock);

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

      list<list<CMsgRecord*>::iterator> udtsend;
      udtsend.clear();

      CGuard::enterCS(self->m_SndQueueLock);
      int64_t ts = CTimer::getTime();
      for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end(); ++i)
      {
         if ((*i)->m_pMsg->m_iLength > m_iMaxUDPMsgSize)
         {
            // Send large message using UDT.
            udtsend.push_back(i);
            continue;
         }

         int64_t diff = ts - (*i)->m_llTimeStamp;
         if ((diff > 10 * 1000000) && ((*i)->m_pMsg->m_piHeader[0] == 0))
         {
            // timeout, send with UDT...
            udtsend.push_back(i);
            //TODO: should probably drop this msg instead of send using UDT
            continue;
         }
         else if (diff > 1000000)
         {
            // Don't send out UDP packets too often.
            self->UDPsend((*i)->m_strIP.c_str(), (*i)->m_iPort, (*i)->m_pMsg);
         }
      }
      CGuard::leaveCS(self->m_SndQueueLock);

      //Use UDT to send large & undelivered messages.
      for (list<list<CMsgRecord*>::iterator>::iterator i = udtsend.begin(); i != udtsend.end(); ++ i)
      {
         // TODO: erase this msg if send failure caused by connection problem.
         if (self->UDTsend((**i)->m_strIP.c_str(), (**i)->m_iPort, (**i)->m_pMsg) >= 0)
         {
            CGuard::enterCS(self->m_SndQueueLock);
            delete (**i)->m_pMsg;
            delete (**i);
            self->m_lSndQueue.erase(*i);
            CGuard::leaveCS(self->m_SndQueueLock);
         }
      }
   }

   return NULL;
}

#ifndef WIN32
void* CGMP::rcvHandler(void* s)
#else
DWORD WINAPI CGMP::rcvHandler(LPVOID s)
#endif
{
   CGMP* const self = (CGMP*)s;

   sockaddr_in addr;
   int32_t header[CGMPMessage::g_iHdrField];
   const int32_t& type = header[0];
   const int32_t& session = header[1];
   const int32_t& src_chn = header[2];
   const int32_t& dst_chn = header[3];
   const int32_t& id = header[4];
   const int32_t& info = header[5];
   char* const buf = new char [m_iMaxUDPMsgSize];

#ifndef WIN32
   iovec vec[2];
   vec[0].iov_base = header;
   vec[0].iov_len = CGMPMessage::g_iHdrSize;
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
   vec[0].len = CGMPMessage::g_iHdrSize;
   vec[1].buf = buf;
   vec[1].len = m_iMaxUDPMsgSize;
#endif

   // acknowledgment to a data or control packet.
   int32_t ack[CGMPMessage::g_iHdrField];
   ack[0] = 1;
   ack[1] = CGMPMessage::g_iSession;
   ack[2] = 0; // src channel ID
   ack[3] = 0; // dst channel ID
   ack[4] = 0;
   ack[5] = 0;

   while (!self->m_bClosed)
   {
      #ifndef WIN32
         int rsize;
         if ((rsize = recvmsg(self->m_UDPSocket, &mh, 0)) < 0)
            continue;
      #else
         int asize = sizeof(sockaddr_in);
         DWORD rsize = m_iMaxUDPMsgSize + CGMPMessage::g_iHdrSize;
         DWORD flag = 0;

         if (0 != WSARecvFrom(self->m_UDPSocket, vec, 2, &rsize, &flag, (sockaddr*)&addr, &asize, NULL, NULL))
            continue;
      #endif

      char ip[NI_MAXHOST];
      char port_str[NI_MAXSERV];
      getnameinfo((sockaddr*)&addr, sizeof(sockaddr_in), ip, sizeof(ip),
                  port_str, sizeof(port_str), NI_NUMERICHOST|NI_NUMERICSERV);
      const int port = atoi(port_str);

      CChannelRec* chn = self->getChnHandle(dst_chn);
      if (NULL == chn)
         continue;

      if (type != 0)
      {
         switch (type)
         {
         case 1: // ACK
            CGuard::enterCS(self->m_SndQueueLock);
            // TODO: optimize this search.
            for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end(); ++ i)
            {
               if (id == (*i)->m_pMsg->m_iID)
               {
                  int rtt = int(CTimer::getTime() - (*i)->m_llTimeStamp);
                  self->m_PeerHistory.insert(ip, port, CGMPMessage::g_iSession, -1, rtt, info);

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
            ack[0] = 1;
            ack[4] = id;
            ack[5] = 0;
            ::sendto(self->m_UDPSocket, (char*)ack, CGMPMessage::g_iHdrSize, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

            break;

         case 3: // rendezvous UDT connection request
         {
            bool match = false;
            CGuard::enterCS(self->m_SndQueueLock);
            // TODO: optimize this search.
            for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end(); ++ i)
            {
               if (id == (*i)->m_pMsg->m_iID)
               {
                  match = true;
                  delete (*i)->m_pMsg;
                  delete (*i);
                  self->m_lSndQueue.erase(i);
                  break;
               }
            }
            CGuard::leaveCS(self->m_SndQueueLock);

            if (!match)
            {
               // Return this message with the UDT port on this side
               // iff the other side does not have the information yet.
               ack[0] = 3;
               ack[4] = id;
               ack[5] = self->m_iUDTReusePort;
               ::sendto(self->m_UDPSocket, (char*)ack, CGMPMessage::g_iHdrSize, 0, (sockaddr*)&addr, sizeof(sockaddr_in));
            }

            // This is not perfect. closed connection from the peer may not be detected.
            UDTSOCKET usock = 0;
            if ((self->m_PeerHistory.getUDTSocket(ip, port, usock) < 0) ||
                (UDT::getsockstate(usock) != CONNECTED))               
            {
               // No existing connection, or connection has been broken.
               // Create a new one.
               self->UDTCreate(usock);
               UDT::epoll_add_usock(self->m_iUDTEPollID, usock);
               self->m_PeerHistory.setUDTSocket(ip, port, usock);
            }

            // TODO: add IPv6 support

            if (UDT::getsockstate(usock) == OPENED)
            {
               // info carries peer udt port.
               self->UDTConnect(usock, ip, info);
            }

            break;
         }

         default:
            break;
         }

         continue;
      }

      // repeated message, send ACK and disgard
      if (self->m_PeerHistory.hit(ip, ntohs(addr.sin_port), session, id))
      {
         ack[0] = 1;
         ack[4] = id;
         ack[5] = 0;
         ::sendto(self->m_UDPSocket, (char*)ack, CGMPMessage::g_iHdrSize, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

         continue;
      }

      CMsgRecord* rec = new CMsgRecord;
      rec->m_strIP = ip;
      rec->m_iPort = port;
      rec->m_pMsg = new CGMPMessage;
      //rec->m_pMsg->m_iType = type;
      rec->m_pMsg->m_iSession = session;
      rec->m_pMsg->m_iSrcChn = src_chn;
      rec->m_pMsg->m_iDstChn = dst_chn;
      rec->m_pMsg->m_iID = id;
      rec->m_pMsg->m_iInfo = info;
      rec->m_pMsg->m_iLength = rsize - CGMPMessage::g_iHdrSize;
      rec->m_pMsg->m_pcData = new char[rec->m_pMsg->m_iLength];
      memcpy(rec->m_pMsg->m_pcData, buf, rec->m_pMsg->m_iLength);

      self->m_PeerHistory.insert(rec->m_strIP, rec->m_iPort, session, id);

      int qsize = 0;
      self->storeMsg(info, chn, rec, qsize);

      ack[0] = 1;
      ack[4] = id;
      ack[5] = qsize; // flow control
      ::sendto(self->m_UDPSocket, (char*)ack, CGMPMessage::g_iHdrSize, 0, (sockaddr*)&addr, sizeof(sockaddr_in));
   }

   delete [] buf;

   // Wake up all blocking recvs.
   for (map<int, CChannelRec*>::iterator i = self->m_mCurrentChn.begin(); i != self->m_mCurrentChn.end(); ++ i)
   {
      #ifndef WIN32
         pthread_cond_signal(&i->second->m_RcvQueueCond);
         pthread_cond_signal(&i->second->m_ResQueueCond);
      #else
         SetEvent(i->second->m_RcvQueueCond);
         SetEvent(i->second->m_ResQueueCond);
      #endif
   }

   return NULL;
}

#ifndef WIN32
void* CGMP::udtRcvHandler(void* s)
#else
DWORD WINAPI CGMP::udtRcvHandler(LPVOID s)
#endif
{
   CGMP* self = (CGMP*)s;

   int32_t header[CGMPMessage::g_iHdrField];

   while (!self->m_bClosed)
   {
      //TODO: use timeout.
      set<UDTSOCKET> readfds;
      UDT::epoll_wait(self->m_iUDTEPollID, &readfds, NULL, -1);

      for (set<UDTSOCKET>::iterator i = readfds.begin(); i != readfds.end(); ++ i)
      {
         int port;
         if (self->UDTRecv(*i, (char*)&port, 4) < 0)
            continue;

         // recv "header" information
         if (self->UDTRecv(*i, (char*)header, CGMPMessage::g_iHdrSize) < 0)
            continue;

         // TODO: this may be retrieved from UDT connection cache as well.
         sockaddr_in addr;
         int addrlen = sizeof(sockaddr_in);
         UDT::getpeername(*i, (sockaddr*)&addr, &addrlen);
         char peer_ip[NI_MAXHOST];
         char peer_udt_port[NI_MAXSERV];
         getnameinfo((sockaddr*)&addr, addrlen, peer_ip, sizeof(peer_ip),
                     peer_udt_port, sizeof(peer_udt_port), NI_NUMERICHOST|NI_NUMERICSERV);

         CMsgRecord* rec = new CMsgRecord;

         rec->m_strIP = peer_ip;
         rec->m_iPort = port;
         rec->m_pMsg = new CGMPMessage;
         //rec->m_pMsg->m_iType = type;
         rec->m_pMsg->m_iSession = header[1];
         rec->m_pMsg->m_iSrcChn = header[2];
         rec->m_pMsg->m_iDstChn = header[3];
         rec->m_pMsg->m_iID = header[4];
         rec->m_pMsg->m_iInfo = header[5];

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

         CChannelRec* chn = self->getChnHandle(rec->m_pMsg->m_iDstChn);
         if (chn == NULL)
         {
            delete rec->m_pMsg;
            delete rec;
            continue;
         }

         if (self->m_PeerHistory.hit(rec->m_strIP, rec->m_iPort, rec->m_pMsg->m_iSession, rec->m_pMsg->m_iID))
            continue;

         self->m_PeerHistory.insert(rec->m_strIP, rec->m_iPort, rec->m_pMsg->m_iSession, rec->m_pMsg->m_iID);

         int tmp;
         self->storeMsg(rec->m_pMsg->m_iInfo, chn, rec, tmp);
      }
   }

   return NULL;
}

int CGMP::rpc(const string& ip, const int& port, CUserMessage* req, CUserMessage* res, const int& src_chn, const int& dst_chn)
{
   int32_t id = 0;
   if (sendto(ip, port, id, req, src_chn, dst_chn) < 0)
      return -1;

   uint64_t t = CTimer::getTime();
   int errcount = 0;

   int tmp;
   while (recv(id, res, &tmp, dst_chn) < 0)
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

int CGMP::multi_rpc(const vector<Address>& dest, CUserMessage* req, vector<CUserMessage*>* res, const int& src_chn, const int& dst_chn)
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
      if (sendto(i->m_strIP, i->m_iPort, id, req, src_chn, dst_chn) < 0)
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

         int tmp_src;
         while (recv(*n, msg, &tmp_src, dst_chn) < 0)
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

int CGMP::createChn()
{
   CGuard cg(m_ChnLock);

   do {
      m_iChnIDSeed ++;
      if (m_iChnIDSeed < 0)
         m_iChnIDSeed = 1;
      if (m_mCurrentChn.find(m_iChnIDSeed) != m_mCurrentChn.end())
         continue;
      m_mCurrentChn[m_iChnIDSeed] = new CChannelRec;
      break;
   } while (true);

   return m_iChnIDSeed;
}

int CGMP::releaseChn(int chn)
{
   CGuard cg(m_ChnLock);
   if (m_mCurrentChn.erase(chn))
      return 0;
   return -1;
}

CChannelRec* CGMP::getChnHandle(int id)
{
   CChannelRec* chn = NULL;
   CGuard::enterCS(m_ChnLock);
   map<int, CChannelRec*>::iterator i = m_mCurrentChn.find(id);
   if (i != m_mCurrentChn.end())
      chn = i->second;
   // TODO: increase reference count.
   CGuard::leaveCS(m_ChnLock);
   return chn;
}

void CGMP::storeMsg(int info, CChannelRec* chn, CMsgRecord* rec, int& qsize)
{
   if (0 == info)
   {
      #ifndef WIN32
         pthread_mutex_lock(&chn->m_RcvQueueLock);
         chn->m_qRcvQueue.push(rec);
         qsize += chn->m_qRcvQueue.size();
         pthread_mutex_unlock(&chn->m_RcvQueueLock);
         pthread_cond_signal(&chn->m_RcvQueueCond);
      #else
         WaitForSingleObject(chn->m_RcvQueueLock, INFINITE);
         chn->m_qRcvQueue.push(rec);
         qsize += chn->m_qRcvQueue.size();
         ReleaseMutex(chn->m_RcvQueueLock);
         SetEvent(chn->m_RcvQueueCond);
      #endif
   }
   else
   {
      #ifndef WIN32
         pthread_mutex_lock(&chn->m_ResQueueLock);
         chn->m_mResQueue[info] = rec;
         pthread_mutex_unlock(&chn->m_ResQueueLock);
         pthread_cond_signal(&chn->m_ResQueueCond);
      #else
         WaitForSingleObject(chn->m_ResQueueLock, INFINITE);
         chn->m_mResQueue[info] = rec;
         ReleaseMutex(chn->m_ResQueueLock);
         SetEvent(chn->m_ResQueueCond);
      #endif
   }
}
