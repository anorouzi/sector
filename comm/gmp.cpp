#include <gmp.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <iostream>

int32_t CGMPMessage::g_iSession = CGMPMessage::initSession();
int32_t CGMPMessage::g_iID = 1;
pthread_mutex_t CGMPMessage::g_IDLock = PTHREAD_MUTEX_INITIALIZER;


CGMPMessage::CGMPMessage():
m_iType(m_piHeader[0]),
m_iSession(m_piHeader[1]),
m_iID(m_piHeader[2]),
m_iInfo(m_piHeader[3]),
m_pcData(NULL),
m_iLength(0)
{
   m_iSession = g_iSession;

   pthread_mutex_lock(&g_IDLock);
   m_iID = g_iID ++;
   if (g_iID == g_iMaxID)
      g_iID = 1;
   pthread_mutex_unlock(&g_IDLock);
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

CGMPMessage::~CGMPMessage()
{
   if (NULL != m_pcData)
      delete [] m_pcData;
}

int32_t CGMPMessage::initSession()
{
   timeval t;
   gettimeofday(&t, 0);

   srand(t.tv_usec);

   return (int32_t)(rand() + 1);
}

CGMP::CGMP()
{
   pthread_mutex_init(&m_SndQueueLock, NULL);
   pthread_cond_init(&m_SndQueueCond, NULL);
   pthread_mutex_init(&m_RcvQueueLock, NULL);
   pthread_cond_init(&m_RcvQueueCond, NULL);
   pthread_mutex_init(&m_ResQueueLock, NULL);
   pthread_cond_init(&m_ResQueueCond, NULL);
   pthread_mutex_init(&m_RTTLock, NULL);
   pthread_cond_init(&m_RTTCond, NULL);

   m_bClosed = false;
}

CGMP::~CGMP()
{
   pthread_mutex_destroy(&m_SndQueueLock);
   pthread_cond_destroy(&m_SndQueueCond);
   pthread_mutex_destroy(&m_RcvQueueLock);
   pthread_cond_destroy(&m_RcvQueueCond);
   pthread_mutex_destroy(&m_ResQueueLock);
   pthread_cond_destroy(&m_ResQueueCond);
   pthread_mutex_destroy(&m_RTTLock);
   pthread_cond_destroy(&m_RTTCond);
}

int CGMP::init(const int& port)
{
   m_iUDPSocket = socket(AF_INET, SOCK_DGRAM, 0);

   m_iTCPSocket = socket(AF_INET, SOCK_STREAM, 0);
   int yes = 1;
   if (-1 == setsockopt(m_iTCPSocket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)))
   {
      perror("setsockopt");
      return -1;
   }

   sockaddr_in addr;

   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(addr.sin_zero), '\0', 8);


   if (0 != ::bind(m_iUDPSocket, (sockaddr *)&addr, sizeof(sockaddr_in)))
   {
      perror("bind");
      return -1;
   }

   timeval tv;
   tv.tv_sec = 1;
   tv.tv_usec = 0;

   setsockopt(m_iUDPSocket, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(timeval));


   socklen_t socklen = sizeof(sockaddr_in);
   if (-1 == getsockname(m_iUDPSocket, (sockaddr *)&addr, &socklen))
   {
      perror("getsockname");
      return -1;
   }

   m_iPort = ntohs(addr.sin_port);


   if (-1 == bind(m_iTCPSocket, (sockaddr*)&addr, sizeof(addr)))
   {
      perror("bind");
      return -1;
   }

   if (-1 == listen(m_iTCPSocket, 10))
   {
      perror("listen");
      return -1;
   }

   pthread_create(&m_SndThread, NULL, sndHandler, this);
   pthread_create(&m_RcvThread, NULL, rcvHandler, this);
   pthread_create(&m_TCPRcvThread, NULL, tcpRcvHandler, this);

   return 1;
}

int CGMP::close()
{
   //cout << "=====================================================to close =======================\n";

   m_bClosed = true;

   pthread_join(m_SndThread, NULL);
   pthread_join(m_RcvThread, NULL);

   //cout << "closed\n";

   ::close(m_iUDPSocket);
   ::close(m_iTCPSocket);

   return 1;
}

int CGMP::sendto(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable)
{
   if (len <= m_iMaxUDPMsgSize)
   {
      return UDPsend(ip, port, id, data, len, reliable);
   }
   else
   {
      return TCPsend(ip, port, id, data, len);
   }
}

int CGMP::UDPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len, const bool& reliable)
{
   sockaddr_in addr;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   if (inet_pton(AF_INET, ip, &(addr.sin_addr)) < 0)
      return -1;
   memset(&(addr.sin_zero), '\0', 8);

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(data, len, id);
   id = msg->m_iID;

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

   if (reliable)
   {
      CMsgRecord* rec = new CMsgRecord;
      strcpy(rec->m_pcIP, ip);
      rec->m_iPort = port;
      rec->m_pMsg = msg;
      gettimeofday(&(rec->m_TimeStamp), 0);

      pthread_mutex_lock(&m_SndQueueLock);
      m_lSndQueue.push_back(rec);
      pthread_mutex_unlock(&m_SndQueueLock);
   }

   sendmsg(m_iUDPSocket, &mh, 0);

   //cout << "send to " << ip << " " << port << " " << msg->m_iSession << " " << msg->m_iID << " " << msg->m_iInfo << " " << msg->m_iLength << endl;

   if (!reliable)
      delete msg;

   return 1;
}

int CGMP::TCPsend(const char* ip, const int& port, int32_t& id, const char* data, const int& len)
{
   int sock = socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = htons(port);
   if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0)
   {
      cout << "incorrect network address:" << ip << endl;
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

   if (0 > ::send(sock, &len, 4, 0))
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

   ::close(sock);
   delete msg;

   return 1;
}

int CGMP::recvfrom(char* ip, int& port, int32_t& id, char* data, int& len)
{
   pthread_mutex_lock(&m_RcvQueueLock);
   while (!m_bClosed && m_qRcvQueue.empty())
      pthread_cond_wait(&m_RcvQueueCond, &m_RcvQueueLock);

   if (m_bClosed)
   {
      pthread_mutex_unlock(&m_RcvQueueLock);
      return -1;
   }

   CMsgRecord* rec = m_qRcvQueue.front();
   m_qRcvQueue.pop();

   pthread_mutex_unlock(&m_RcvQueueLock);

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
   timeval now;
   timespec timeout;
   gettimeofday(&now, 0);
   timeout.tv_sec = now.tv_sec + 1;
   timeout.tv_nsec = now.tv_usec * 1000;

   pthread_mutex_lock(&m_ResQueueLock);

   if (0 == m_mResQueue.size())
      pthread_cond_timedwait(&m_ResQueueCond, &m_ResQueueLock, &timeout);

   map<int32_t, CMsgRecord*>::iterator m = m_mResQueue.find(id);

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

   pthread_mutex_unlock(&m_ResQueueLock);

   if (!found)
      return -1;

   return len;
}

void* CGMP::sndHandler(void* s)
{
   CGMP* self = (CGMP*)s;

   timeval currtime;

   while (!self->m_bClosed)
   {
      sleep(1);

      pthread_mutex_lock(&self->m_SndQueueLock);

      for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end();)
      {
         gettimeofday(&currtime, 0);
         if (currtime.tv_sec - (*i)->m_TimeStamp.tv_sec > 10)
         {
            // timeout, send with TCP...

            list<CMsgRecord*>::iterator j = i;
            i ++;

            int sock = socket(AF_INET, SOCK_STREAM, 0);

            if (-1 == sock)
            {
               delete (*j)->m_pMsg;
               delete (*j);
               self->m_lSndQueue.erase(j);

               continue;
            }

            sockaddr_in serv_addr;
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons((*j)->m_iPort);
            inet_pton(AF_INET, (*j)->m_pcIP, &serv_addr.sin_addr);
            memset(&(serv_addr.sin_zero), '\0', 8);

            if (-1 == ::connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr)));
            {
               delete (*j)->m_pMsg;
               delete (*j);
               self->m_lSndQueue.erase(j);

               ::close(sock);

               continue;
            }

            if ((-1 == ::send(sock, (char*)(&(self->m_iPort)), 4, 0)) ||
                (-1 == ::send(sock, (char*)((*j)->m_pMsg->m_piHeader), 16, 0)) ||
                (-1 == ::send(sock, &((*j)->m_pMsg->m_iLength), 4, 0)))
            {
               delete (*j)->m_pMsg;
               delete (*j);
               self->m_lSndQueue.erase(j);

               ::close(sock);

               continue;
            }

            int ssize = 0;
            while (ssize < (*j)->m_pMsg->m_iLength)
            {
               int s;
               if (0 > (s = ::send(sock, (*j)->m_pMsg->m_pcData + ssize, (*j)->m_pMsg->m_iLength - ssize, 0)))
               {
                  delete (*j)->m_pMsg;
                  delete (*j);
                  self->m_lSndQueue.erase(j);

                  ::close(sock);

                  break;
               }
               ssize += s;
            }

            delete (*j)->m_pMsg;
            delete (*j);
            self->m_lSndQueue.erase(j);

            ::close(sock);

            continue;
         }

         sockaddr_in addr;
         addr.sin_family = AF_INET;
         addr.sin_port = htons((*i)->m_iPort);
         inet_pton(AF_INET, (*i)->m_pcIP, &(addr.sin_addr));
         memset(&(addr.sin_zero), '\0', 8);

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

         sendmsg(self->m_iUDPSocket, &mh, 0);

         // check next msg
         ++ i;
      }

      pthread_mutex_unlock(&self->m_SndQueueLock);
   }

   return NULL;
}

void* CGMP::rcvHandler(void* s)
{
   CGMP* self = (CGMP*)s;

   msghdr mh;

   sockaddr_in addr;
   int32_t header[4];
   int32_t& type = header[0];
   int32_t& session = header[1];
   int32_t& id = header[2];
   int32_t& info = header[3];
   char buf[1456];

   iovec vec[2];
   vec[0].iov_base = header;
   vec[0].iov_len = 16;
   vec[1].iov_base = buf;
   vec[1].iov_len = 1456;

   mh.msg_name = &addr;
   mh.msg_namelen = sizeof(sockaddr_in);
   mh.msg_iov = vec;
   mh.msg_iovlen = 2;
   mh.msg_control = NULL;
   mh.msg_controllen = 0;
   mh.msg_flags = 0;

   int32_t ack[4];
   ack[0] = 1;
   ack[1] = CGMPMessage::g_iSession;
   ack[2] = 0;

   int rsize;

   while (!self->m_bClosed)
   {
      rsize = recvmsg(self->m_iUDPSocket, &mh, 0);
      if (rsize < 0)
      {
         continue;
      }

      //cout << "===============> " << type << " " << session << " " << id << " " << info << " " << rsize << endl;

      if (type != 0)
      {
         switch (type)
         {
         case 1: // ACK
            pthread_mutex_lock(&self->m_SndQueueLock);
            for (list<CMsgRecord*>::iterator i = self->m_lSndQueue.begin(); i != self->m_lSndQueue.end(); ++ i)
            {
               if (id == (*i)->m_pMsg->m_iID)
               {
                  timeval currtime;
                  gettimeofday(&currtime, 0);
                  int rtt = (currtime.tv_sec - (*i)->m_TimeStamp.tv_sec) * 1000000 + currtime.tv_usec - (*i)->m_TimeStamp.tv_usec;

                  char ip[64];
                  if (NULL != inet_ntop(AF_INET, &(addr.sin_addr), ip, 64))
                     self->m_PeerHistory.insert(ip, ntohs(addr.sin_port), session, -1, rtt);

                  pthread_cond_signal(&self->m_RTTCond);

                  delete (*i)->m_pMsg;
                  delete (*i);
                  self->m_lSndQueue.erase(i);
                  break;
               }
            }
            pthread_mutex_unlock(&self->m_SndQueueLock);

            break;

         case 2: // RTT probe
            //cout << "RECV RTT PROBE\n";

            ack[2] = id;
            ack[3] = 0;
            ::sendto(self->m_iUDPSocket, ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

            break;

         default:
            break;
         }

         continue;
      }

      // check repeated ID!!!!
      char ip[64];
      if (NULL == inet_ntop(AF_INET, &(addr.sin_addr), ip, 64))
      {
         perror("inet_ntop");
         continue;
      }
      int32_t lastid = self->m_PeerHistory.getLastID(ip, ntohs(addr.sin_port), session);

      if ((lastid >= 0) && (((id <= lastid) && (lastid - id < (1 << 29))) || ((id > lastid) && (id - lastid > (1 << 29)))))
      {
         ack[2] = id;
         ack[3] = 0;
         ::sendto(self->m_iUDPSocket, ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));

         continue;
      }


      CMsgRecord* rec = new CMsgRecord;

      if (NULL == inet_ntop(AF_INET, &(addr.sin_addr), rec->m_pcIP, 64))
      {
         perror("inet_ntop");
         delete rec;
         continue;
      }
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
         pthread_mutex_lock(&self->m_RcvQueueLock);
         self->m_qRcvQueue.push(rec);
         pthread_mutex_unlock(&self->m_RcvQueueLock);

         pthread_cond_signal(&self->m_RcvQueueCond);
      }
      else
      {
         pthread_mutex_lock(&self->m_ResQueueLock);
         self->m_mResQueue[info] = rec;
         pthread_mutex_unlock(&self->m_ResQueueLock);

         pthread_cond_signal(&self->m_ResQueueCond);
      }

      ack[2] = id;
      ack[3] = 0;
      ::sendto(self->m_iUDPSocket, ack, 16, 0, (sockaddr*)&addr, sizeof(sockaddr_in));
   }

   pthread_cond_signal(&self->m_RcvQueueCond);
   pthread_cond_signal(&self->m_ResQueueCond);

   return NULL;
}

void* CGMP::tcpRcvHandler(void* s)
{
   CGMP* self = (CGMP*)s;

   sockaddr_in addr;
   socklen_t namelen = sizeof(sockaddr_in);

   int sock = -1;

   int32_t header[4];

   while (!self->m_bClosed)
   {
      if (-1 == (sock = ::accept(self->m_iTCPSocket, (sockaddr*)&addr, &namelen)))
         continue;

      int port;
      if (0 > ::recv(sock, &port, 4, MSG_WAITALL))
      {
         perror("recv");
         ::close(sock);
         continue;
      }

      // recv "header" information
      if (0 > ::recv(sock, header, 16, MSG_WAITALL))
      {
         perror("recv");
         ::close(sock);
         continue;
      }

      CMsgRecord* rec = new CMsgRecord;

      if (NULL == inet_ntop(AF_INET, &(addr.sin_addr), rec->m_pcIP, 64))
      {
         perror("inet_ntop");
         ::close(sock);
         delete rec;
         continue;
      }
      rec->m_iPort = port;
      rec->m_pMsg = new CGMPMessage;
      //rec->m_pMsg->m_iType = type;
      rec->m_pMsg->m_iSession = header[1];
      rec->m_pMsg->m_iID = header[2];
      rec->m_pMsg->m_iInfo = header[3];

      // recv parameter size
      if (0 > ::recv(sock, (char*)&(rec->m_pMsg->m_iLength), 4, MSG_WAITALL))
      {
         perror("recv");
         ::close(sock);
         delete rec->m_pMsg;
         delete rec;
         continue;
      }

      rec->m_pMsg->m_pcData = new char[rec->m_pMsg->m_iLength];

      int rsize = 0;
      while ((rec->m_pMsg->m_iLength > 0) && (rsize < rec->m_pMsg->m_iLength))
      {
         int r;

         if (0 > (r = ::recv(sock, rec->m_pMsg->m_pcData + rsize, rec->m_pMsg->m_iLength - rsize, MSG_WAITALL)))
         {
            perror("recv");
            break;
         }

         rsize += r;
      }
      if (rsize < rec->m_pMsg->m_iLength)
      {
         ::close(sock);
         delete rec->m_pMsg;
         delete rec;
         continue;
      }


      self->m_PeerHistory.insert(rec->m_pcIP, rec->m_iPort, header[1], header[2]);


      if (0 == header[3])
      {
         pthread_mutex_lock(&self->m_RcvQueueLock);
         self->m_qRcvQueue.push(rec);
         pthread_mutex_unlock(&self->m_RcvQueueLock);

         pthread_cond_signal(&self->m_RcvQueueCond);
      }
      else
      {
         pthread_mutex_lock(&self->m_ResQueueLock);
         self->m_mResQueue[header[3]] = rec;
         pthread_mutex_unlock(&self->m_ResQueueLock);

         pthread_cond_signal(&self->m_ResQueueCond);
      }

      ::close(sock);
   }

   pthread_cond_signal(&self->m_RcvQueueCond);
   pthread_cond_signal(&self->m_ResQueueCond);

   return NULL;
}

int CGMP::sendto(const char* ip, const int& port, int32_t& id, const CUserMessage* msg)
{
   return sendto(ip, port, id, msg->m_pcBuffer, msg->m_iDataLength);
}

int CGMP::recvfrom(char* ip, int& port, int32_t& id, CUserMessage* msg)
{
   int rsize = msg->m_iBufLength;
   recvfrom(ip, port, id, msg->m_pcBuffer, rsize);

   if (rsize > 0)
      msg->m_iDataLength = rsize;
   else
      msg->m_iDataLength = 0;

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
      if (rtt(ip, port) < 0)
         return -1;
   }

   return 1;
}

int CGMP::rtt(const char* ip, const int& port)
{
   int r = m_PeerHistory.getRTT(ip);

   if (r > 0)
      return r;

   sockaddr_in addr;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   if (inet_pton(AF_INET, ip, &(addr.sin_addr)) < 0)
      return -1;
   memset(&(addr.sin_zero), '\0', 8);

   CGMPMessage* msg = new CGMPMessage;
   msg->pack(2, 0);

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

   CMsgRecord* rec = new CMsgRecord;
   strcpy(rec->m_pcIP, ip);
   rec->m_iPort = port;
   rec->m_pMsg = msg;
   gettimeofday(&(rec->m_TimeStamp), 0);

   pthread_mutex_lock(&m_SndQueueLock);
   m_lSndQueue.push_back(rec);
   pthread_mutex_unlock(&m_SndQueueLock);

   sendmsg(m_iUDPSocket, &mh, 0);

   timeval now;
   timespec timeout;
   gettimeofday(&now, 0);
   timeout.tv_sec = now.tv_sec + 1;
   timeout.tv_nsec = now.tv_usec * 1000;
   pthread_mutex_lock(&m_RTTLock);
   pthread_cond_timedwait(&m_RTTCond, &m_RTTLock, &timeout);
   pthread_mutex_unlock(&m_RTTLock);

   return m_PeerHistory.getRTT(ip);
}
