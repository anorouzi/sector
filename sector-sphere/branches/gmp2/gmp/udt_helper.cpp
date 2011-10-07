/*****************************************************************************
Copyright (c) 2005 - 2010, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu, last updated 12/31/2010
*****************************************************************************/

#ifndef WIN32
   #include <netdb.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif
#include <sstream>

#include "gmp.h"
#include "udt.h"

#include <iostream>
using namespace std;


int CGMP::UDTCreate(UDTSOCKET& usock)
{
   addrinfo hints;
   addrinfo* res;

   memset(&hints, 0, sizeof(struct addrinfo));
   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;

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

   if (m_iUDTReusePort == 0)
   {
      // First use, obtain random port, reuse it later.
      // TODO: add IPv6 support.
      sockaddr_in my_addr;
      int size = sizeof(sockaddr_in);
      UDT::getsockname(usock, (sockaddr*)&my_addr, &size);
      m_iUDTReusePort = ntohs(my_addr.sin_port);
   }

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
   int ssize = 0;
   while (ssize < size)
   {
      int ss = UDT::send(usock, buf + ssize, size - ssize, 0);
      if (UDT::ERROR == ss)
         return -1;

      ssize += ss;
   }

   return ssize;
}

int CGMP::UDTRecv(const UDTSOCKET& usock, char* buf, const int& size)
{
   int rsize = 0;
   while (rsize < size)
   {
      int rs = UDT::recv(usock, buf + rsize, size - rsize, 0);
      if (UDT::ERROR == rs)
         return -1;

      rsize += rs;
   }

   return rsize;
}
