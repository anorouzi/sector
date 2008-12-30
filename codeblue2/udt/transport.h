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


#ifndef __CB_TRANSPORT_H__
#define __CB_TRANSPORT_H__

#include <udt.h>
#include <crypto.h>

class Transport
{
public:
   Transport();
   ~Transport();

public:
   static void initialize();
   static void release();

   int open(int& port, bool rendezvous = true, bool reuseaddr = false);

   int listen();
   int accept(Transport& t, sockaddr* addr = NULL, int* addrlen = NULL);

   int connect(const char* ip, int port);
   int send(const char* buf, int size);
   int recv(char* buf, int size);
   int64_t sendfile(std::ifstream& ifs, int64_t offset, int64_t size);
   int64_t recvfile(std::ofstream& ofs, int64_t offset, int64_t size);
   int close();

public: // secure data/file transfer
   int initCoder(unsigned char key[16], unsigned char iv[8]);
   int releaseCoder();

   int secure_send(const char* buf, int size);
   int secure_recv(char* buf, int size);
   int64_t secure_sendfile(std::ifstream& ifs, int64_t offset, int64_t size, const char* key);
   int64_t secure_recvfile(std::ofstream& ofs, int64_t offset, int64_t size, const char* key);

private:
   UDTSOCKET m_Socket;

   Crypto m_Encoder;
   Crypto m_Decoder;
};

#endif
