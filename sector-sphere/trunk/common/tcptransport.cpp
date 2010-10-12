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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef WIN32
   #include <sys/types.h>
   #include <sys/socket.h>
   #include <netinet/in.h>
   #include <netdb.h>
   #include <arpa/inet.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif
#include <sector.h>
#include <tcptransport.h>
#include <cstring>
#include <iostream>
#include <fstream>

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

   int reuse = 1;
   ::setsockopt(m_iSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse));

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

   inet_ntop(AF_INET, &(addr.sin_addr), ip, 64);
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
   return closesocket(m_iSocket);
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

   ifs.seekg(offset);

   int block = 1000000;
   char* buf = new char[block];
   int64_t sent = 0;
   while (sent < size)
   {
      int unit = int((size - sent) > block ? block : size - sent);
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

   ofs.seekp(offset);

   int block = 1000000;
   char* buf = new char[block];
   int64_t recd = 0;
   while (recd < size)
   {
      int unit = int((size - recd) > block ? block : size - recd);
      recv(buf, unit);
      ofs.write(buf, unit);
      recd += unit;
   }

   delete [] buf;
   ofs.close();
   return recd;
}

int TCPTransport::getLocalIP(string& ip)
{
   sockaddr_in addr;
   socklen_t size = sizeof(sockaddr_in);

   if (getsockname(m_iSocket, (sockaddr*)&addr, &size) < 0)
      return -1;

   char tmp[64];

   ip = inet_ntop(AF_INET, &(addr.sin_addr), tmp, 64);

   return 1;
}
