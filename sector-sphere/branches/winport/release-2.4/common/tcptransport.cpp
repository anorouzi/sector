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
   Yunhong Gu, last updated 06/12/2010
*****************************************************************************/

#include <sector.h>
#include <tcptransport.h>
#include <sys/types.h>
#ifndef WIN32
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <netdb.h>
    #include <arpa/inet.h>
#else
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <wspiapi.h>
#endif
#include <cstring>
#include <iostream>
#include <fstream>
#include "common.h"

using namespace std;

TCPTransport::TCPTransport():
m_iSocket(0),
m_bConnected(false)
{

}

TCPTransport::~TCPTransport()
{

}

int TCPTransport::open(const char* ip, const int& port)
{
   if ((m_iSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
      return SectorError::E_RESOURCE;

   if ((NULL == ip) && (0 == port))
      return 0;

   sockaddr_in addr;
   memset(&addr, 0, sizeof(sockaddr_in));
   addr.sin_addr.s_addr = INADDR_ANY;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);

   const char reuse = 1;
   ::setsockopt(m_iSocket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

   if (::bind(m_iSocket, (sockaddr*)&addr, sizeof(sockaddr_in)) < 0)
   {
      cerr << "TCP socket unable to bind on address " << ip << " " << port << endl;
      return SectorError::E_RESOURCE;
   }

   return 0;
}

int TCPTransport::listen()
{
   return ::listen(m_iSocket, 1024);
}

TCPTransport* TCPTransport::accept(char* ip, int& port)
{
   TCPTransport* t = new TCPTransport;

   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);
   if ((t->m_iSocket = ::accept(m_iSocket, (sockaddr*)&addr, &size)) < 0)
      return NULL;

   udt_inet_ntop(AF_INET, &(addr.sin_addr), ip, 64);
   port = addr.sin_port;

   t->m_bConnected = true;

   return t;
}

int TCPTransport::connect(const char* host, const int& port)
{
   if (m_bConnected)
      return 0;

   sockaddr_in addr;
   addr.sin_family = AF_INET;
   addr.sin_port = htons(port);
   hostent* he = gethostbyname(host);

   if (NULL == he)
   {
      cerr << "SSL connect: invalid address " << host << " " << port << endl;
      return SectorError::E_CONNECTION;
   }

   addr.sin_addr.s_addr = ((in_addr*)he->h_addr)->s_addr;
   memset(addr.sin_zero, '\0', sizeof(addr.sin_zero));

   if (::connect(m_iSocket, (sockaddr*)&addr, sizeof(sockaddr_in)) < 0)
   {
      cerr << "TCP connect: unable to connect to server.\n";
      return SectorError::E_CONNECTION;
   }

   m_bConnected = true;

   return 1;
}

int TCPTransport::close()
{
   if (!m_bConnected)
      return 0;

   m_bConnected = false;
#ifndef WIN32
   return ::close(m_iSocket);
#else
   return ::closesocket(m_iSocket);
#endif
}

int TCPTransport::send(const char* data, const int& size)
{
   if (!m_bConnected)
      return -1;

   int ts = size;
   while (ts > 0)
   {
      int s = ::send(m_iSocket, data + size - ts, ts, 0);
      if (s <= 0)
         return -1;
      ts -= s;
   }

   return size;
}

int TCPTransport::recv(char* data, const int& size)
{
   if (!m_bConnected)
      return -1;

   int tr = size;
   while (tr > 0)
   {
      int r = ::recv(m_iSocket, data + size - tr, tr, 0);
      if (r <= 0)
         return -1;
      tr -= r;
   }

   return size;
}

int64_t TCPTransport::sendfile(const char* file, const int64_t& offset, const int64_t& size)
{
   if (!m_bConnected)
      return -1;

   ifstream ifs(file, ios::in | ios::binary);

   if (ifs.bad() || ifs.fail())
      return -1;

   int block = 1000000;
   char* buf = new char[block];
   int64_t sent = 0;
   while (sent < size)
   {
      int unit = (size - sent) > block ? block : static_cast<int>(size - sent);
      ifs.read(buf, unit);
      send(buf, unit);
      sent += unit;
   }

   delete [] buf;
   ifs.close();

   return sent;
}

int64_t TCPTransport::recvfile(const char* file, const int64_t& offset, const int64_t& size)
{
   if (!m_bConnected)
      return -1;

   fstream ofs(file, ios::out | ios::binary);

   if (ofs.bad() || ofs.fail())
      return -1;

   int block = 1000000;
   char* buf = new char[block];
   int64_t recd = 0;
   while (recd < size)
   {
      int unit = (size - recd) > block ? block : static_cast<int>(size - recd);
      recv(buf, unit);
      ofs.write(buf, unit);
      recd += unit;
   }

   delete [] buf;
   ofs.close();
   return recd;
}

int TCPTransport::getLocalIP(std::string& ip)
{
   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);

   if (getsockname(m_iSocket, (sockaddr*)&addr, &size) < 0)
      return -1;

   char tmp[64];

   ip = udt_inet_ntop(AF_INET, &(addr.sin_addr), tmp, 64);

   return 1;
}
