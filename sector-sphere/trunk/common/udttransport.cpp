/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 01/02/2011
*****************************************************************************/


#ifndef WIN32
   #include <sys/types.h>
   #include <sys/socket.h>
   #include <arpa/inet.h>
   #include <netdb.h>
#else
   #include <windows.h>
#endif

#include <fstream>
#include <cstring>
#include <cstdlib>
#include "udttransport.h"

using namespace std;

UDTTransport::UDTTransport()
{
}

UDTTransport::~UDTTransport()
{
}

void UDTTransport::initialize()
{
   UDT::startup();
}

void UDTTransport::release()
{
   UDT::cleanup();
}

int UDTTransport::open(int& port, bool rendezvous, bool reuseaddr)
{
   m_Socket = UDT::socket(AF_INET, SOCK_STREAM, 0);

   if (UDT::INVALID_SOCK == m_Socket)
      return -1;

   UDT::setsockopt(m_Socket, 0, UDT_REUSEADDR, &reuseaddr, sizeof(bool));

   sockaddr_in my_addr;
   my_addr.sin_family = AF_INET;
   my_addr.sin_port = htons(port);
   my_addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(my_addr.sin_zero), '\0', 8);

   if (UDT::bind(m_Socket, (sockaddr*)&my_addr, sizeof(my_addr)) == UDT::ERROR)
      return -1;

   int size = sizeof(sockaddr_in);
   UDT::getsockname(m_Socket, (sockaddr*)&my_addr, &size);
   port = ntohs(my_addr.sin_port);

   #ifdef WIN32
      int mtu = 1052;
      UDT::setsockopt(m_Socket, 0, UDT_MSS, &mtu, sizeof(int));
   #endif

   UDT::setsockopt(m_Socket, 0, UDT_RENDEZVOUS, &rendezvous, sizeof(bool));

   return 1;
}

int UDTTransport::listen()
{
   return UDT::listen(m_Socket, 1024);
}

UDTTransport* UDTTransport::accept(string& ip, int& port)
{
   timeval tv;
   UDT::UDSET readfds;

   tv.tv_sec = 0;
   tv.tv_usec = 10000;

   UD_ZERO(&readfds);
   UD_SET(m_Socket, &readfds);

   int res = UDT::select(1, &readfds, NULL, NULL, &tv);

   if ((res == UDT::ERROR) || (!UD_ISSET(m_Socket, &readfds)))
      return NULL;

   UDTTransport* t = new UDTTransport;

   sockaddr_in addr;
   int addrlen = sizeof(addr);
   t->m_Socket = UDT::accept(m_Socket, (sockaddr*)&addr, &addrlen);

   if (t->m_Socket == UDT::INVALID_SOCK)
   {
      delete t;
      return NULL;
   }

   char clienthost[NI_MAXHOST];
   char clientport[NI_MAXSERV];
   getnameinfo((sockaddr*)&addr, addrlen, clienthost, sizeof(clienthost), clientport, sizeof(clientport), NI_NUMERICHOST|NI_NUMERICSERV);

   ip = clienthost;
   port = atoi(clientport);

   return t;
}

int UDTTransport::connect(const string& ip, int port)
{
   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = htons(port);
   #ifndef WIN32
      inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr);
   #else
      serv_addr.sin_addr.s_addr = inet_addr(ip.c_str());
   #endif
   memset(&(serv_addr.sin_zero), '\0', 8);

   if (UDT::ERROR == UDT::connect(m_Socket, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      return -1;

   return 1;
}

int UDTTransport::close()
{
   return UDT::close(m_Socket);
}

int UDTTransport::send(const char* buf, int size)
{
   int ssize = 0;
   while (ssize < size)
   {
      int ss = UDT::send(m_Socket, buf + ssize, size - ssize, 0);
      if (UDT::ERROR == ss)
         return -1;

      ssize += ss;
   }

   return ssize;
}

int UDTTransport::recv(char* buf, int size)
{
   int rsize = 0;
   while (rsize < size)
   {
      int rs = UDT::recv(m_Socket, buf + rsize, size - rsize, 0);
      if (UDT::ERROR == rs)
         return -1;

      rsize += rs;
   }

   return rsize;
}

int64_t UDTTransport::sendfile(fstream& ifs, int64_t offset, int64_t size)
{
   return UDT::sendfile(m_Socket, ifs, offset, size);
}

int64_t UDTTransport::recvfile(fstream& ifs, int64_t offset, int64_t size)
{
   return UDT::recvfile(m_Socket, ifs, offset, size);
}

bool UDTTransport::isConnected()
{
   return (UDT::send(m_Socket, NULL, 0, 0) == 0);
}

int64_t UDTTransport::getRealSndSpeed()
{
   UDT::TRACEINFO perf;
   if (UDT::perfmon(m_Socket, &perf) < 0)
      return -1;

   if (perf.usSndDuration <= 0)
      return -1;

   int mss;
   int size = sizeof(int);
   UDT::getsockopt(m_Socket, 0, UDT_MSS, &mss, &size);
   return int64_t(8.0 * perf.pktSent * mss / (perf.usSndDuration / 1000000.0));
}

int UDTTransport::getLocalAddr(std::string& ip, int& port)
{
   sockaddr_in addr;
   int size = sizeof(sockaddr_in);

   if (UDT::getsockname(m_Socket, (sockaddr*)&addr, &size) < 0)
      return -1;

   char clienthost[NI_MAXHOST];
   char clientport[NI_MAXSERV];
   getnameinfo((sockaddr*)&addr, size, clienthost, sizeof(clienthost), clientport, sizeof(clientport), NI_NUMERICHOST|NI_NUMERICSERV);

   ip = clienthost;
   port = atoi(clientport);

   return 0;
}
