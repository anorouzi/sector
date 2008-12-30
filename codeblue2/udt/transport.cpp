/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 12/01/2008
*****************************************************************************/


#ifndef WIN32
   #include <sys/types.h>
   #include <sys/socket.h>
   #include <arpa/inet.h>
#else
   #include <windows.h>
#endif

#include <fstream>
#include "transport.h"

using namespace std;

Transport::Transport()
{
}

Transport::~Transport()
{
}

void Transport::initialize()
{
   UDT::startup();
}

void Transport::release()
{
   UDT::cleanup();
}

int Transport::open(int& port, bool rendezvous, bool reuseaddr)
{
   m_Socket = UDT::socket(AF_INET, SOCK_STREAM, 0);

   UDT::setsockopt(m_Socket, 0, UDT_REUSEADDR, &reuseaddr, sizeof(bool));

   sockaddr_in my_addr;
   my_addr.sin_family = AF_INET;
   my_addr.sin_port = htons(port);
   my_addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(my_addr.sin_zero), '\0', 8);

   UDT::bind(m_Socket, (sockaddr*)&my_addr, sizeof(my_addr));

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

int Transport::listen()
{
   return UDT::listen(m_Socket, 10);
}

int Transport::accept(Transport& t, sockaddr* addr, int* addrlen)
{
   timeval tv;
   UDT::UDSET readfds;

   tv.tv_sec = 0;
   tv.tv_usec = 10000;

   UD_ZERO(&readfds);
   UD_SET(m_Socket, &readfds);

   int res = UDT::select(1, &readfds, NULL, NULL, &tv);

   if ((res == UDT::ERROR) || (!UD_ISSET(m_Socket, &readfds)))
      return -1;

   t.m_Socket = UDT::accept(m_Socket, addr, addrlen);

   if (t.m_Socket == UDT::INVALID_SOCK)
      return -1;

   return 0;
}

int Transport::connect(const char* ip, int port)
{
   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = htons(port);
   #ifndef WIN32
      inet_pton(AF_INET, ip, &serv_addr.sin_addr);
   #else
      serv_addr.sin_addr.s_addr = inet_addr(ip);
   #endif
      memset(&(serv_addr.sin_zero), '\0', 8);

   if (UDT::ERROR == UDT::connect(m_Socket, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      return -1;

   return 1;
}

int Transport::send(const char* buf, int size)
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

int Transport::recv(char* buf, int size)
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

int64_t Transport::sendfile(ifstream& ifs, int64_t offset, int64_t size)
{
   return UDT::sendfile(m_Socket, ifs, offset, size);
}

int64_t Transport::recvfile(ofstream& ifs, int64_t offset, int64_t size)
{
   return UDT::recvfile(m_Socket, ifs, offset, size);
}

int Transport::close()
{
   return UDT::close(m_Socket);
}

int Transport::initCoder(unsigned char key[16], unsigned char iv[16])
{
   m_Encoder.initEnc(key, iv);
   m_Decoder.initDec(key, iv);
}

int Transport::releaseCoder()
{
   m_Encoder.release();
   m_Encoder.release();
}

int Transport::secure_send(const char* buf, int size)
{
}

int Transport::secure_recv(char* buf, int size)
{
}

int64_t Transport::secure_sendfile(std::ifstream& ifs, int64_t offset, int64_t size, const char* key)
{
}

int64_t Transport::secure_recvfile(std::ofstream& ofs, int64_t offset, int64_t size, const char* key)
{
}
