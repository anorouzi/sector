/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Group Messaging Protocol (GMP)

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/07/2007
*****************************************************************************/


#ifndef WIN32
   #include <unistd.h>
   #include <stdio.h>
   #include <errno.h>
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
   #ifdef WIN32
      WORD wVersionRequested;
      WSADATA wsaData;
      int err;
      wVersionRequested = MAKEWORD(2, 2);
      if (0 != WSAStartup(wVersionRequested, &wsaData))
         return -1;
   #endif

   m_UDPSocket = socket(AF_INET, SOCK_DGRAM, 0);

   m_TCPSocket = socket(AF_INET, SOCK_STREAM, 0);
   int yes = 1;
   if (-1 == setsockopt(m_TCPSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&yes, sizeof(int)))
   {
      perror("setsockopt");
      return -1;
   }

   sockaddr_in addr;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(addr.sin_zero), '\0', 8);

   if (0 != ::bind(m_UDPSocket, (sockaddr *)&addr, sizeof(sockaddr_in)))
   {
      perror("bind");
      return -1;
   }

   timeval tv;
   tv.tv_sec = 1;
   tv.tv_usec = 0;

   setsockopt(m_UDPSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(timeval));

   socklen_t socklen = sizeof(sockaddr_in);
   if (-1 == getsockname(m_UDPSocket, (sockaddr *)&addr, &socklen))
   {
      perror("getsockname");
      return -1;
   }

   m_iPort = ntohs(addr.sin_port);

   if (-1 == bind(m_TCPSocket, (sockaddr*)&addr, sizeof(addr)))
   {
      perror("bind");
      return -1;
   }

   if (-1 == listen(m_TCPSocket, 10))
   {
      perror("listen");
      return -1;
   }

   #ifndef WIN32
      pthread_create(&m_SndThread, NULL, sndHandler, this);
      pthread_create(&m_RcvThread, NULL, rcvHandler, this);
      pthread_create(&m_TCPRcvThread, NULL, tcpRcvHandler, this);
   #else
      m_SndThread = CreateThread(NULL, 0, sndHandler, this, 0, NULL);
      m_RcvThread = CreateThread(NULL, 0, rcvHandler, this, 0, NULL);
      m_TCPRcvThread = CreateThread(NULL, 0, tcpRcvHandler, this, 0, NULL);
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
   #else
      SetEvent(m_SndQueueCond);
      WaitForSingleObject(m_SndThread, INFINITE);
      WaitForSingleObject(m_RcvThread, INFINITE);
   #endif

   closesocket(m_UDPSocket);
   closesocket(m_TCPSocket);

   #ifdef WIN32
      WSACleanup();
   #endif

   return 1;
}

int CGMP::sendto(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable)
{
   if (len <= m_iMaxUDPMsgSize)
      return UDPsend(ip, port, id, data, len, reliable);
   else
      return TCPsend(ip, port, id, data, len);
}

int CGMP::UDPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable)
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

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(data, len, id);
   id = msg->m_iID;

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

//   cout << "send to " << ip << " " << port << " " << msg->m_iSession << " " << msg->m_iID << " " << msg->m_iInfo << " " << msg->m_iLength << endl;

   if (!reliable)
      delete msg;

   return 1;
}

int CGMP::TCPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len)
{
   #ifndef WIN32
      int sock;
   #else
      SOCKET sock;
   #endif
   sock = socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = htons(port);
   #ifndef WIN32
      if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0)
   #else
      if (INADDR_NONE == (serv_addr.sin_addr.s_addr = inet_addr(ip)))
   #endif
   {
      //cout << "incorrect network address:" << ip << endl;
      return -1;
   }
   memset(&(serv_addr.sin_zero), '\0', 8);

   // connect to the server, implict bind
   if (-1 == ::connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
   {
      //cout << "why TCP c f " << ip << " " << port << endl;

      perror("TCP connect");
      return -1;
   }

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(data, len, id);
   id = msg->m_iID;

   //cout << "TCP PORT sent " << m_iPort << endl; 
   if (0 > ::send(sock, (char*)(&m_iPort), 4, 0))
   {
      perror("send");
      return -1;
   }
   
   if (0 > ::send(sock, (char*)(msg->m_piHeader), 16, 0))
   {
      perror("send");
      return -1;
   }

   if (0 > ::send(sock, (char*)&len, 4, 0))
   {
      perror("send");
      return -1;
   }

   int ssize = 0;
   while (ssize < len)
   {
      int s;
      if (0 > (s = ::send(sock, data + ssize, len - ssize, 0)))
      {
         perror("send");
         return -1;
      }
      ssize += s;
   }

   //cout << "sent by TCP " << ip << " " << port << " " << msg->m_iSession << " " << msg->m_iID << " " << msg->m_iInfo << " " <<m_lSndQueue.size() << endl;

   closesocket(sock);
   delete msg;
   return 1;
}

int CGMP::recvfrom(char* ip, int& port, int32_t& id, char* data, int& len, const bool& block)
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
      len = 0;
      return -1;
   }

   CMsgRecord* rec = m_qRcvQueue.front();
   m_qRcvQueue.pop();

   Sync::leaveCS(m_RcvQueueLock);

   strcpy(ip, rec->m_pcIP);
   port = rec->m_iPort;
   id = rec->m_pMsg->m_iID;

   if (len > rec->m_pMsg->m_iLength)
      len = rec->m_pMsg->m_iLength;
   memcpy(data, rec->m_pMsg->m_pcData, len);

   delete rec->m_pMsg;
   delete rec;

   return len;
}

int CGMP::recv(const int32_t& id, char* data, int& len)
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
      if (len > m->second->m_pMsg->m_iLength)
         len = m->second->m_pMsg->m_iLength;
      if (len > 0)
         memcpy(data, m->second->m_pMsg->m_pcData, len);
      delete m->second->m_pMsg;
      delete m->second;
      m_mResQueue.erase(m);

      found = true;
   }

   Sync::leaveCS(m_ResQueueLock);

   if (!found)
      return -1;

   return len;
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
      Sync::enterCS(self->m_SndQueueLock);

      #ifndef WIN32
         timespec timeout;
         timeval now;
         gettimeofday(&now, 0);
         timeout.tv_sec = now.tv_sec + 1;
         timeout.tv_nsec = now.tv_usec * 1000;
         pthread_cond_timedwait(&self->m_SndQueueCond, &self->m_SndQueueLock, &timeout);
      #else
         ReleaseMutex(self->m_SndQueueLock);
         WaitForSingleObject(self->m_SndQueueCond, 1000);
         WaitForSingleObject(self->m_SndQueueLock, INFINITE);
      #endif

      for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end();)
      {
         if (Time::getTime() - (*i)->m_llTimeStamp > 10 * 1000000)
         {
            // timeout, send with TCP...

            list<CMsgRecord*>::iterator j = i;
            i ++;

            #ifndef WIN32
               int sock;
            #else
               SOCKET sock;
            #endif
            sock = socket(AF_INET, SOCK_STREAM, 0);

            if (-1 == sock)
               goto UDP_END;

            sockaddr_in serv_addr;
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons((*j)->m_iPort);
            #ifndef WIN32
               inet_pton(AF_INET, (*j)->m_pcIP, &serv_addr.sin_addr);
            #else
               serv_addr.sin_addr.s_addr = inet_addr((*j)->m_pcIP);
            #endif
            memset(&(serv_addr.sin_zero), '\0', 8);

            if (-1 == ::connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
               goto UDP_END;

            if ((-1 == ::send(sock, (char*)(&(self->m_iPort)), 4, 0)) ||
                (-1 == ::send(sock, (char*)((*j)->m_pMsg->m_piHeader), 16, 0)) ||
                (-1 == ::send(sock, (char*)&((*j)->m_pMsg->m_iLength), 4, 0)))
            {
               goto UDP_END;
            }

            for (int ssize = 0; ssize < (*j)->m_pMsg->m_iLength;)
            {
               int s;
               if (0 > (s = ::send(sock, (*j)->m_pMsg->m_pcData + ssize, (*j)->m_pMsg->m_iLength - ssize, 0)))
                  goto UDP_END;
               ssize += s;
            }

            UDP_END:
            delete (*j)->m_pMsg;
            delete (*j);
            self->m_lSndQueue.erase(j);
            closesocket(sock);
            continue;
         }

         sockaddr_in addr;
         addr.sin_family = AF_INET;
         addr.sin_port = htons((*i)->m_iPort);
         #ifndef WIN32
            inet_pton(AF_INET, (*i)->m_pcIP, &(addr.sin_addr));
         #else
            addr.sin_addr.s_addr = inet_addr((*i)->m_pcIP);
         #endif
         memset(&(addr.sin_zero), '\0', 8);

#ifndef WIN32
         iovec vec[2];
         vec[0].iov_base = (*i)->m_pMsg->m_piHeader;
         vec[0].iov_len = 16;
         vec[1].iov_base = (*i)->m_pMsg->m_pcData;
         vec[1].iov_len = (*i)->m_pMsg->m_iLength;

         msghdr mh;
         mh.msg_name = &addr;
         mh.msg_namelen = sizeof(sockaddr_in);
         mh.msg_iov = vec;
         mh.msg_iovlen = 2;
         mh.msg_control = NULL;
         mh.msg_controllen = 0;
         mh.msg_flags = 0;

         sendmsg(self->m_UDPSocket, &mh, 0);
#else
         WSABUF vec[2];
         vec[0].buf = (char*)(*i)->m_pMsg->m_piHeader;
         vec[0].len = 16;
         vec[1].buf = (*i)->m_pMsg->m_pcData;
         vec[1].len = (*i)->m_pMsg->m_iLength;

         DWORD ssize;
         WSASendTo(self->m_UDPSocket, vec, 2, &ssize, 0, (sockaddr*)&addr, sizeof(sockaddr_in), NULL, NULL);
#endif

         // check next msg
         ++ i;
      }

      Sync::leaveCS(self->m_SndQueueLock);
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

      //cout << "===============> " << type << " " << session << " " << id << " " << info << " " << rsize << endl;

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
            //cout << "RECV RTT PROBE\n";

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
void* CGMP::tcpRcvHandler(void* s)
#else
DWORD WINAPI CGMP::tcpRcvHandler(LPVOID s)
#endif
{
   CGMP* self = (CGMP*)s;

   sockaddr_in addr;
   socklen_t namelen = sizeof(sockaddr_in);

   #ifndef WIN32
      int sock;
   #else
      SOCKET sock;
   #endif

   int32_t header[4];

   while (!self->m_bClosed)
   {
      sock = ::accept(self->m_TCPSocket, (sockaddr*)&addr, &namelen);
      #ifndef WIN32
         if (-1 == sock)
             continue;
      #else
         if (INVALID_SOCKET == sock)
             continue;
      #endif

      int port;
      if (0 > ::recv(sock, (char*)&port, 4, 0 /*MSG_WAITALL*/))
      {
         perror("recv");
         closesocket(sock);
         continue;
      }

      // recv "header" information
      if (0 > ::recv(sock, (char*)header, 16, 0 /*MSG_WAITALL*/))
      {
         perror("recv");
         closesocket(sock);
         continue;
      }

      CMsgRecord* rec = new CMsgRecord;

      #ifndef WIN32
         if (NULL == inet_ntop(AF_INET, &(addr.sin_addr), rec->m_pcIP, 64))
         {
            perror("inet_ntop");
            closesocket(sock);
            delete rec;
            continue;
         }
      #else
         char* tmp = NULL;
         if (NULL == (tmp = inet_ntoa(addr.sin_addr)))
         {
            perror("inet_ntoa");
            closesocket(sock);
            delete rec;
            continue;
         }
         strncpy(rec->m_pcIP, tmp, 64);
      #endif

      rec->m_iPort = port;
      rec->m_pMsg = new CGMPMessage;
      //rec->m_pMsg->m_iType = type;
      rec->m_pMsg->m_iSession = header[1];
      rec->m_pMsg->m_iID = header[2];
      rec->m_pMsg->m_iInfo = header[3];

      // recv parameter size
      if (0 > ::recv(sock, (char*)&(rec->m_pMsg->m_iLength), 4, 0 /*MSG_WAITALL*/))
      {
         perror("recv");
         closesocket(sock);
         delete rec->m_pMsg;
         delete rec;
         continue;
      }

      rec->m_pMsg->m_pcData = new char[rec->m_pMsg->m_iLength];

      int rsize = 0;
      while ((rec->m_pMsg->m_iLength > 0) && (rsize < rec->m_pMsg->m_iLength))
      {
         int r;

         if (0 > (r = ::recv(sock, rec->m_pMsg->m_pcData + rsize, rec->m_pMsg->m_iLength - rsize, 0 /*MSG_WAITALL*/)))
         {
            perror("recv");
            break;
         }

         rsize += r;
      }
      if (rsize < rec->m_pMsg->m_iLength)
      {
         closesocket(sock);
         delete rec->m_pMsg;
         delete rec;
         continue;
      }

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

      closesocket(sock);
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

int CGMP::sendto(const char* ip, const int& port, int32_t& id, const CUserMessage* msg)
{
   return sendto(ip, port, id, msg->m_pcBuffer, msg->m_iDataLength);
}

int CGMP::recvfrom(char* ip, int& port, int32_t& id, CUserMessage* msg, const bool& block)
{
   int rsize = msg->m_iBufLength;
   if (recvfrom(ip, port, id, msg->m_pcBuffer, rsize, block) < 0)
   {
      msg->m_iDataLength = 0;
      return -1;
   }

   msg->m_iDataLength = rsize;

   return rsize;
}

int CGMP::recv(const int32_t& id, CUserMessage* msg)
{
   int rsize = msg->m_iBufLength;

   if (recv(id, msg->m_pcBuffer, rsize) < 0)
      return -1;

   if (rsize > 0)
      msg->m_iDataLength = rsize;
   else
      msg->m_iDataLength = 0;

   return rsize;
}

int CGMP::rpc(const char* ip, const int& port, CUserMessage* req, CUserMessage* res)
{
   int32_t id = 0;
   if (sendto(ip, port, id, req) < 0)
      return -1;

   while (recv(id, res) < 0)
   {
      if (rtt(ip, port, true) < 0)
         return -1;
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

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(2, 0);

   CMsgRecord* rec = new CMsgRecord;
   strcpy(rec->m_pcIP, ip);
   rec->m_iPort = port;
   rec->m_pMsg = msg;
   rec->m_llTimeStamp = Time::getTime();

   Sync::enterCS(m_SndQueueLock);
   m_lSndQueue.push_back(rec);
   Sync::leaveCS(m_SndQueueLock);

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

   timeval now;
   timespec timeout;
   gettimeofday(&now, 0);
   timeout.tv_sec = now.tv_sec + 1;
   timeout.tv_nsec = now.tv_usec * 1000;
   pthread_mutex_lock(&m_RTTLock);
   pthread_cond_timedwait(&m_RTTCond, &m_RTTLock, &timeout);
   pthread_mutex_unlock(&m_RTTLock);
#else
   WSABUF vec[2];
   vec[0].buf = (char*)msg->m_piHeader;
   vec[0].len = 16;
   vec[1].buf = msg->m_pcData;
   vec[1].len = msg->m_iLength;

   DWORD ssize;
   WSASendTo(m_UDPSocket, vec, 2, &ssize, 0, (sockaddr*)&addr, sizeof(sockaddr_in), NULL, NULL);

   WaitForSingleObject(m_RTTCond, 1000);
#endif

   return m_PeerHistory.getRTT(ip);
}
